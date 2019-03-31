// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use failure::format_err;
use futures::{future, Async, Future, Stream};
use serde::{Deserialize, Serialize};
use std::fmt;

use metastore::{DataflowEvent, MetaStore};
use ore::closure;

mod util;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
struct DummyDataflow(String);

#[test]
fn test_basic() -> Result<(), failure::Error> {
    ore::log::init();

    let prefix = "metastore-test-basic";
    util::zk_delete_all(prefix)?;

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let (ms1, ms2, mut watch1a, mut watch2) = runtime
        .block_on(future::lazy(move || {
            let ms1 = MetaStore::new(&util::ZOOKEEPER_ADDR, prefix);
            let ms2 = MetaStore::new(&util::ZOOKEEPER_ADDR, prefix);
            let watch1a = ms1.register_dataflow_watch();
            ms1.create_dataflow("basic", &DummyDataflow("basic".into()))
                .and_then(move |_| {
                    ms2.create_dataflow("basic2", &DummyDataflow("basic2".into()))
                        .map(|_| {
                            let watch2 = ms2.register_dataflow_watch();
                            (ms2, watch2)
                        })
                })
                .and_then(|(ms2, watch2)| {
                    let futs: Vec<_> = (0..5)
                        .map(|i| {
                            let name = format!("concurrent{}", i);
                            ms2.create_dataflow(&name, &DummyDataflow(name.clone()))
                        })
                        .collect();
                    future::join_all(futs).map(move |_| (ms2, watch2))
                })
                .map(move |(ms2, watch2)| (ms1, ms2, watch1a, watch2))
        }))
        .unwrap();

    // Create a watch after dataflows are created.
    let mut watch1b = ms1.register_dataflow_watch();

    // Verify that all watches see the same events.
    assert_events(
        vec![&mut watch1a, &mut watch1b, &mut watch2],
        &[
            DataflowEvent::Created(DummyDataflow("basic".into())),
            DataflowEvent::Created(DummyDataflow("basic2".into())),
            DataflowEvent::Created(DummyDataflow("concurrent0".into())),
            DataflowEvent::Created(DummyDataflow("concurrent1".into())),
            DataflowEvent::Created(DummyDataflow("concurrent2".into())),
            DataflowEvent::Created(DummyDataflow("concurrent3".into())),
            DataflowEvent::Created(DummyDataflow("concurrent4".into())),
        ],
    );

    // Delete a dataflow.
    runtime
        .block_on(future::lazy(closure!([clone ms1] || {
            ms1.delete_dataflow("basic")
        })))
        .unwrap();

    // Verify that all watches see the deletion.
    assert_events(
        vec![&mut watch1a, &mut watch1b, &mut watch2],
        &[DataflowEvent::Deleted("basic".into())],
    );

    // Create a new watch, after the deletion, and verify that it sees a
    // compacted sequence of events.
    let mut watch1c = ms1.register_dataflow_watch();
    assert_events(
        vec![&mut watch1c],
        &[
            DataflowEvent::Created(DummyDataflow("basic2".into())),
            DataflowEvent::Created(DummyDataflow("concurrent0".into())),
            DataflowEvent::Created(DummyDataflow("concurrent1".into())),
            DataflowEvent::Created(DummyDataflow("concurrent2".into())),
            DataflowEvent::Created(DummyDataflow("concurrent3".into())),
            DataflowEvent::Created(DummyDataflow("concurrent4".into())),
        ],
    );

    // Drop the MetaStores, which will cancel any background futures they've
    // spawned, so that we can cleanly shutdown.
    drop(ms1);
    drop(ms2);

    // TODO(benesch): this might be cleaner when uninhabited types land, since
    // the return type of wait could be Result<(), !>.
    // See: https://github.com/rust-lang/rust/issues/35121
    runtime
        .shutdown_on_idle()
        .wait()
        .map_err(|()| format_err!("unreachable!"))?;

    // Verify that the watchers didn't produce any stray events.
    assert_eq!(watch1a.poll(), Ok(Async::Ready(None)));
    assert_eq!(watch1b.poll(), Ok(Async::Ready(None)));
    assert_eq!(watch2.poll(), Ok(Async::Ready(None)));
    Ok(())
}

fn assert_events<S>(streams: S, events: &[DataflowEvent<DummyDataflow>])
where
    S: IntoIterator,
    S::Item: Stream<Item = DataflowEvent<DummyDataflow>>,
    <S::Item as Stream>::Error: fmt::Debug,
{
    let n = events.len() as u64;
    let streams = streams.into_iter().map(|s| s.take(n).collect());
    let results = future::join_all(streams).wait().unwrap();
    for (i, mut res) in results.into_iter().enumerate() {
        res.sort();
        assert_eq!(res, events, "watcher {} event mismatch", i);
    }
}
