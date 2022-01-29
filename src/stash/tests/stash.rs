// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::error::Error;

use ore::metrics::MetricsRegistry;
use tempfile::{NamedTempFile, TempDir};

use mz_stash::{PersistStash, SqliteStash, Stash, StashOp};
use persist::file::{FileBlob, FileLog};
use persist::storage::{Blob, LockInfo};

#[test]
fn test_sqlite() -> Result<(), anyhow::Error> {
    let file = NamedTempFile::new()?;
    run_tests(|| SqliteStash::open(file.path()))?;
    Ok(())
}

#[test]
fn test_persist() -> Result<(), anyhow::Error> {
    let dir = TempDir::new()?;
    run_tests(|| {
        let lock_info = LockInfo::new("stash".into(), "nonce".into())?;
        let log = FileLog::new(dir.path().join("log"), lock_info.clone())?;
        let blob = FileBlob::open_exclusive(dir.path().join("blob").into(), lock_info)?;
        PersistStash::open(blob, log, &MetricsRegistry::new())
    })?;
    Ok(())
}

fn run_tests<F, E, S>(open_stash: F) -> Result<(), anyhow::Error>
where
    F: Fn() -> Result<S, E>,
    E: Error + Send + Sync + 'static,
    S: Stash<String, String>,
{
    let expected = HashSet::from_iter([
        ("a".into(), "2".into()),
        ("c".into(), "5".into()),
        ("d".into(), "4".into()),
    ]);

    let mut stash = open_stash()?;
    stash.write_batch(vec![
        StashOp::Put("a".into(), "1".into()),
        StashOp::Put("b".into(), "2".into()),
        StashOp::Put("a".into(), "2".into()),
        StashOp::Put("c".into(), "3".into()),
        StashOp::Put("d".into(), "4".into()),
    ])?;
    stash.put("c".into(), "5".into())?;
    stash.delete("b".into())?;

    let snapshot: HashSet<_> = stash.replay()?.collect();
    assert_eq!(snapshot, expected);

    let stash = open_stash()?;
    let snapshot: HashSet<_> = stash.replay()?.collect();
    assert_eq!(snapshot, expected);

    Ok(())
}
