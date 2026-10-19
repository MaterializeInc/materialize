// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::time::Duration;

use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::Metrics;
use mz_persist_client::{Diagnostics, PersistLocation};
use mz_persist_types::ShardId;
use mz_repr::{Datum, SqlScalarType};
use mz_storage_operators::persist::SharedBatches;
use mz_storage_types::controller::CollectionMetadata;
use timely::dataflow::operators::{Input, Inspect};

use crate::metrics::ComputeMetrics;
use crate::server::ComputeRuntimeRole;

use super::*;

fn corrections() -> Corrections {
    let registry = MetricsRegistry::new();
    let metrics = Metrics::new(&PersistConfig::new_for_tests(), &registry);
    let sink_metrics = metrics.sink.clone();
    let worker_metrics = sink_metrics.for_worker(0);
    let config = mz_dyncfgs::all_dyncfgs();
    OkErr::new(
        Correction::new(sink_metrics.clone(), worker_metrics.clone(), None, &config),
        Correction::new(sink_metrics, worker_metrics, None, &config),
    )
}

#[mz_ore::test]
fn write_batch_yields_while_the_write_task_is_full() {
    let mut corrections = corrections();
    let mut updates = (0..3)
        .map(|i| {
            (
                Row::pack_slice(&[Datum::Int64(i)]),
                Timestamp::from(0),
                Diff::from(1),
            )
        })
        .collect();
    corrections.ok.insert(&mut updates);
    let desc = BatchDescription::new(
        Antichain::from_elem(Timestamp::from(0)),
        Antichain::from_elem(Timestamp::from(1)),
        0,
    );
    let key_schema = Rc::new(
        RelationDesc::builder()
            .with_column("a", SqlScalarType::Int64.nullable(false))
            .finish(),
    );
    let metrics = WriteMetrics {
        wait_seconds: GenericCounter::new("wait", "wait").expect("valid metric"),
        part_bytes: IntCounter::new("bytes", "bytes").expect("valid metric"),
    };
    // A channel of one, and a part per update.
    let (tx, mut rx) = mpsc::channel(1);
    let mut write = Box::pin(write_batch(
        corrections,
        desc,
        key_schema,
        1,
        Rc::new(tx),
        metrics,
    ));

    let mut cx = Context::from_waker(Waker::noop());
    let mut commands = Vec::new();
    let mut pending = 0;
    let corrections = loop {
        match write.as_mut().poll(&mut cx) {
            Poll::Ready(corrections) => break corrections,
            Poll::Pending => {
                pending += 1;
                let cmd = rx
                    .try_recv()
                    .expect("pending only while the channel is full");
                commands.push(cmd);
            }
        }
    };
    while let Ok(cmd) = rx.try_recv() {
        commands.push(cmd);
    }

    assert!(pending > 0, "a full channel makes the write pending");
    let shape: Vec<_> = commands
        .iter()
        .map(|cmd| match cmd {
            WriteCommand::Start(_) => "start",
            WriteCommand::Part(_) => "part",
            WriteCommand::Finish => "finish",
        })
        .collect();
    assert_eq!(shape, ["start", "part", "part", "part", "finish"]);
    let upper = Antichain::from_elem(Timestamp::from(1));
    assert_eq!(
        corrections.ok.consolidated_updates_before(&upper).count(),
        3,
        "the write hands the corrections back unchanged"
    );
}

fn relation_desc() -> RelationDesc {
    RelationDesc::builder()
        .with_column("a", SqlScalarType::Int64.nullable(false))
        .finish()
}

fn row(i: i64) -> Row {
    Row::pack_slice(&[Datum::Int64(i)])
}

fn ts(t: u64) -> Timestamp {
    Timestamp::from(t)
}

fn batch_description(lower: u64, upper: u64) -> BatchDescription {
    BatchDescription::new(
        Antichain::from_elem(ts(lower)),
        Antichain::from_elem(ts(upper)),
        0,
    )
}

/// Drives the `write` operator through a batch write that waits on a full write task channel.
///
/// The write task runs on a current-thread runtime that only makes progress while the test blocks
/// on it, so the test decides when the write task takes the operator's commands.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn write_operator_absorbs_updates_after_a_pending_write() {
    let counts = timely::execute_directly(|worker| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("valid runtime");
        let _guard = runtime.enter();

        let persist_clients = Arc::new(PersistClientCache::new_no_metrics());
        let collection = CollectionMetadata {
            persist_location: PersistLocation::new_in_mem(),
            data_shard: ShardId::new(),
            relation_desc: relation_desc(),
            txns_shard: None,
        };
        let persist_api = PersistApi {
            persist_clients: Arc::clone(&persist_clients),
            persist_batches: SharedBatches::new(),
            collection: collection.clone(),
            shard_name: "test".to_string(),
            purpose: "test".to_string(),
        };
        let compute_metrics =
            ComputeMetrics::register_with(&MetricsRegistry::new(), ComputeRuntimeRole::Solo)
                .for_worker(0);
        let part_bytes = compute_metrics.mv_sink_write_part_bytes_total.clone();
        let (_read_only_tx, read_only_rx) = watch::channel(false);
        let written = Rc::new(RefCell::new(Vec::new()));

        let dataflow_index = worker.next_dataflow_index();
        let (mut desired, mut persist, mut descs) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (desired_ok, desired_ok_stream) = scope.new_input();
            let (desired_err, desired_err_stream) = scope.new_input();
            let (persist_ok, persist_ok_stream) = scope.new_input();
            let (persist_err, persist_err_stream) = scope.new_input();
            let (descs, descs_stream) = scope.new_input();
            let written = Rc::clone(&written);
            render(
                GlobalId::User(1),
                persist_api,
                Antichain::from_elem(ts(0)),
                OkErr::new(desired_ok_stream, desired_err_stream),
                OkErr::new(persist_ok_stream, persist_err_stream),
                descs_stream,
                read_only_rx,
                Rc::new(mz_dyncfgs::all_dyncfgs()),
                &compute_metrics,
            )
            .inspect(move |(_desc, batch)| written.borrow_mut().push(batch.clone()));
            (
                OkErr::new(desired_ok, desired_err),
                OkErr::new(persist_ok, persist_err),
                descs,
            )
        });
        // Steps the worker without running the write task, which therefore takes no commands.
        let step = |worker: &mut timely::worker::Worker| {
            for _ in 0..10 {
                worker.step();
            }
        };
        // Steps the worker and runs the write task until `done` holds.
        let run_until = |worker: &mut timely::worker::Worker, done: &dyn Fn() -> bool| {
            for _ in 0..1000 {
                if done() {
                    return true;
                }
                runtime.block_on(tokio::time::sleep(Duration::from_millis(1)));
                worker.step();
            }
            done()
        };

        // The first batch has one part, so its `Start` and `Part` fill the channel of
        // `PARTS_IN_FLIGHT` commands and its `Finish` waits.
        desired.ok.send((row(0), ts(0), Diff::ONE));
        desired.ok.advance_to(ts(1));
        desired.err.advance_to(ts(1));
        descs.send(batch_description(0, 1));
        descs.advance_to(ts(1));
        step(worker);
        assert!(part_bytes.get() > 0, "the first batch write started");

        // Updates that arrive while the batch write is pending.
        desired.ok.send((row(1), ts(1), Diff::ONE));
        desired.ok.advance_to(ts(2));
        desired.err.advance_to(ts(2));
        step(worker);
        assert!(
            written.borrow().is_empty(),
            "the first batch write is pending"
        );

        assert!(
            run_until(worker, &|| written.borrow().len() == 1),
            "the write task taking a command re-activates the operator"
        );

        // Feed the first batch back as if it were appended.
        persist.ok.send((row(0), ts(0), Diff::ONE));
        persist.ok.advance_to(ts(1));
        persist.err.advance_to(ts(1));
        descs.send(batch_description(1, 2));
        descs.advance_to(ts(2));
        assert!(
            run_until(worker, &|| written.borrow().len() == 2),
            "the second batch holds the updates that arrived during the first write"
        );

        // Dropping the runtime drops the write task, which the operator treats as a failure, so
        // drop the operator first.
        worker.drop_dataflow(dataflow_index);

        let batches = written.borrow().clone();
        runtime.block_on(async {
            let writer = persist_clients
                .open(collection.persist_location.clone())
                .await
                .expect("valid location")
                .open_writer::<SourceData, (), Timestamp, StorageDiff>(
                    collection.data_shard,
                    Arc::new(relation_desc()),
                    Arc::new(UnitSchema),
                    Diagnostics::for_tests(),
                )
                .await
                .expect("valid usage");
            batches
                .into_iter()
                .map(|proto| {
                    writer
                        .batch_from_transmittable_batch(proto)
                        .into_hollow_batch()
                        .len
                })
                .collect::<Vec<_>>()
        })
    });
    assert_eq!(counts, [1, 1], "each batch holds one update");
}
