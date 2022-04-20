// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tempfile::NamedTempFile;
use timely::progress::Antichain;

use mz_stash::{Sqlite, Stash, Timestamp};

#[test]
fn test_stash_sqlite() -> Result<(), anyhow::Error> {
    let file = NamedTempFile::new()?;
    let conn = Sqlite::open(file.path())?;
    test_stash(conn)
}

fn test_stash<S: Stash>(mut stash: S) -> Result<(), anyhow::Error> {
    // Create an arrangement, write some data into it, then read it back.
    let orders = stash.collection::<String, String>("orders")?;
    stash.update(orders, ("widgets".into(), "1".into()), 1, 1)?;
    stash.update(orders, ("wombats".into(), "2".into()), 1, 2)?;
    assert_eq!(
        stash.iter(orders)?,
        &[
            (("widgets".into(), "1".into()), 1, 1),
            (("wombats".into(), "2".into()), 1, 2),
        ]
    );
    assert_eq!(
        stash.iter_key(orders, &"widgets".to_string())?,
        &[("1".into(), 1, 1)]
    );
    assert_eq!(
        stash.iter_key(orders, &"wombats".to_string())?,
        &[("2".into(), 1, 2)]
    );

    // Write to another arrangement and ensure the data stays separate.
    let other = stash.collection::<String, String>("other")?;
    stash.update(other, ("foo".into(), "bar".into()), 1, 1)?;
    assert_eq!(stash.iter(other)?, &[(("foo".into(), "bar".into()), 1, 1)],);
    assert_eq!(
        stash.iter(orders)?,
        &[
            (("widgets".into(), "1".into()), 1, 1),
            (("wombats".into(), "2".into()), 1, 2),
        ]
    );

    // Check that consolidation happens immediately...
    stash.update(orders, ("wombats".into(), "2".into()), 1, -1)?;
    assert_eq!(
        stash.iter(orders)?,
        &[
            (("widgets".into(), "1".into()), 1, 1),
            (("wombats".into(), "2".into()), 1, 1),
        ]
    );

    // ...even when it results in a entry's removal.
    stash.update(orders, ("wombats".into(), "2".into()), 1, -1)?;
    assert_eq!(
        stash.iter(orders)?,
        &[(("widgets".into(), "1".into()), 1, 1),]
    );

    // Check that logical compaction applies immediately.
    stash.update_many(
        orders,
        [
            (("widgets".into(), "1".into()), 2, 1),
            (("widgets".into(), "1".into()), 3, 1),
            (("widgets".into(), "1".into()), 4, 1),
        ],
    )?;
    stash.seal(orders, Antichain::from_elem(3).borrow())?;
    stash.compact(orders, Antichain::from_elem(3).borrow())?;
    assert_eq!(
        stash.iter(orders)?,
        &[
            (("widgets".into(), "1".into()), 3, 3),
            (("widgets".into(), "1".into()), 4, 1),
        ]
    );

    // Check that physical compaction does not change the collection's contents.
    stash.consolidate(orders)?;
    assert_eq!(
        stash.iter(orders)?,
        &[
            (("widgets".into(), "1".into()), 3, 3),
            (("widgets".into(), "1".into()), 4, 1),
        ]
    );

    // Test invalid seals, compactions, and updates.
    assert_eq!(
        stash
            .seal(orders, Antichain::from_elem(2).borrow())
            .unwrap_err()
            .to_string(),
        "stash error: seal request {2} is less than the current upper frontier {3}",
    );
    assert_eq!(
        stash
            .compact(orders, Antichain::from_elem(2).borrow())
            .unwrap_err()
            .to_string(),
        "stash error: compact request {2} is less than the current since frontier {3}",
    );
    assert_eq!(
        stash
            .compact(orders, Antichain::from_elem(4).borrow())
            .unwrap_err()
            .to_string(),
        "stash error: compact request {4} is greater than the current upper frontier {3}",
    );
    assert_eq!(
        stash
            .update(orders, ("wodgets".into(), "1".into()), 2, 1)
            .unwrap_err()
            .to_string(),
        "stash error: entry time 2 is less than the current upper frontier {3}",
    );

    // Test advancing since and upper to the empty frontier.
    stash.seal(orders, Antichain::new().borrow())?;
    stash.compact(orders, Antichain::new().borrow())?;
    assert_eq!(
        match stash.iter(orders) {
            Ok(_) => panic!("call to iter unexpectedly succeeded"),
            Err(e) => e.to_string(),
        },
        "stash error: cannot iterate collection with empty since frontier",
    );
    assert_eq!(
        match stash.iter_key(orders, &"wombats".to_string()) {
            Ok(_) => panic!("call to iter_key unexpectedly succeeded"),
            Err(e) => e.to_string(),
        },
        "stash error: cannot iterate collection with empty since frontier",
    );
    stash.consolidate(orders)?;

    // Double check that the other collection is still untouched.
    assert_eq!(stash.iter(other)?, &[(("foo".into(), "bar".into()), 1, 1)],);
    assert_eq!(stash.since(other)?, Antichain::from_elem(Timestamp::MIN));
    assert_eq!(stash.upper(other)?, Antichain::from_elem(Timestamp::MIN));

    Ok(())
}
