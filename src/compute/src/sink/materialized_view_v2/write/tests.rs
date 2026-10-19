// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::Metrics;
use mz_repr::{Datum, SqlScalarType};

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
