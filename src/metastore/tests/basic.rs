// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use failure::format_err;
use futures::{future, Future, Stream};
use serde::{Deserialize, Serialize};

use metastore::MetaStore;

mod util;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
struct DummyDataflow(String);

#[test]
fn test_basic() -> Result<(), failure::Error> {
    ore::log::init();

    let prefix = "metastore-test-basic";
    util::zk_delete_all(prefix)?;

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let (ms1, ms2, watch1a, watch2) = runtime
        .block_on(future::lazy(move || {
            let ms1 = MetaStore::new(&util::ZOOKEEPER_ADDR, prefix);
            let ms2 = MetaStore::new(&util::ZOOKEEPER_ADDR, prefix);
            let watch1a = ms1.register_dataflow_watch();
            ms1.new_dataflow("basic", &DummyDataflow("basic".into()))
                .and_then(move |_| {
                    ms2.new_dataflow("basic2", &DummyDataflow("basic2".into()))
                        .map(|_| {
                            let watch2 = ms2.register_dataflow_watch();
                            (ms2, watch2)
                        })
                })
                .and_then(|(ms2, watch2)| {
                    let futs: Vec<_> = (0..5)
                        .map(|i| {
                            let name = format!("concurrent{}", i);
                            ms2.new_dataflow(&name, &DummyDataflow(name.clone()))
                        })
                        .collect();
                    future::join_all(futs).map(move |_| (ms2, watch2))
                })
                .map(move |(ms2, watch2)| (ms1, ms2, watch1a, watch2))
        }))
        .unwrap();

    // Test creating a watch after dataflows are created.
    let watch1b = ms1.register_dataflow_watch();

    let mut iter1a = watch1a.wait().map(|r| r.unwrap());
    let mut iter1b = watch1b.wait().map(|r| r.unwrap());
    let mut iter2 = watch2.wait().map(|r| r.unwrap());

    // Verify that all watchers saw all the expected events. Note that we don't
    // care about ordering.
    let expected_events = &[
        DummyDataflow("basic".into()),
        DummyDataflow("basic2".into()),
        DummyDataflow("concurrent0".into()),
        DummyDataflow("concurrent1".into()),
        DummyDataflow("concurrent2".into()),
        DummyDataflow("concurrent3".into()),
        DummyDataflow("concurrent4".into()),
    ];
    let mut events1a: Vec<_> = (&mut iter1a).take(7).collect();
    let mut events1b: Vec<_> = (&mut iter1b).take(7).collect();
    let mut events2: Vec<_> = (&mut iter2).take(7).collect();
    events1a.sort();
    events1b.sort();
    events2.sort();
    assert_eq!(events1a, expected_events);
    assert_eq!(events1b, expected_events);
    assert_eq!(events2, expected_events);

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
    assert_eq!(iter1a.count(), 0);
    assert_eq!(iter1b.count(), 0);
    assert_eq!(iter2.count(), 0);
    Ok(())
}
