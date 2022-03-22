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

use mz_stash::Stash;

#[test]
fn test_stash() -> Result<(), anyhow::Error> {
    let file = NamedTempFile::new()?;
    let stash = Stash::open(file.path())?;

    // Create an arrangement, write some data into it, then read it back.
    let mut orders = stash.collection::<String, String>("orders")?;
    orders.update(("widgets".into(), "1".into()), 1, 1)?;
    orders.update(("wombats".into(), "2".into()), 1, 2)?;
    assert_eq!(
        orders.iter()?.collect::<Vec<_>>(),
        &[
            (("widgets".into(), "1".into()), 1, 1),
            (("wombats".into(), "2".into()), 1, 2),
        ]
    );
    assert_eq!(
        orders.iter_key("widgets".into())?.collect::<Vec<_>>(),
        &[("1".into(), 1, 1)]
    );
    assert_eq!(
        orders.iter_key("wombats".into())?.collect::<Vec<_>>(),
        &[("2".into(), 1, 2)]
    );

    // Write to another arrangement and ensure the data stays separate.
    let mut other = stash.collection::<String, String>("other")?;
    other.update(("foo".into(), "bar".into()), 1, 1)?;
    assert_eq!(
        other.iter()?.collect::<Vec<_>>(),
        &[(("foo".into(), "bar".into()), 1, 1)],
    );
    assert_eq!(
        orders.iter()?.collect::<Vec<_>>(),
        &[
            (("widgets".into(), "1".into()), 1, 1),
            (("wombats".into(), "2".into()), 1, 2),
        ]
    );

    // Check that consolidation happens immediately...
    orders.update(("wombats".into(), "2".into()), 1, -1)?;
    assert_eq!(
        orders.iter()?.collect::<Vec<_>>(),
        &[
            (("widgets".into(), "1".into()), 1, 1),
            (("wombats".into(), "2".into()), 1, 1),
        ]
    );

    // ...even when it results in a entry's removal.
    orders.update(("wombats".into(), "2".into()), 1, -1)?;
    assert_eq!(
        orders.iter()?.collect::<Vec<_>>(),
        &[(("widgets".into(), "1".into()), 1, 1),]
    );

    // Check that logical compaction applies immediately.
    orders.update_many([
        (("widgets".into(), "1".into()), 2, 1),
        (("widgets".into(), "1".into()), 3, 1),
        (("widgets".into(), "1".into()), 4, 1),
    ])?;
    orders.seal(Antichain::from_elem(3).borrow())?;
    orders.compact(Antichain::from_elem(3).borrow())?;
    assert_eq!(
        orders.iter()?.collect::<Vec<_>>(),
        &[
            (("widgets".into(), "1".into()), 3, 3),
            (("widgets".into(), "1".into()), 4, 1),
        ]
    );

    // Check that physical compaction does not change the collection's contents.
    orders.consolidate()?;
    assert_eq!(
        orders.iter()?.collect::<Vec<_>>(),
        &[
            (("widgets".into(), "1".into()), 3, 3),
            (("widgets".into(), "1".into()), 4, 1),
        ]
    );

    // Test invalid seals, compactions, and updates.
    assert_eq!(
        orders
            .seal(Antichain::from_elem(2).borrow())
            .unwrap_err()
            .to_string(),
        "stash error: seal request {2} is less than the current upper frontier {3}",
    );
    assert_eq!(
        orders
            .compact(Antichain::from_elem(2).borrow())
            .unwrap_err()
            .to_string(),
        "stash error: compact request {2} is less than the current since frontier {3}",
    );
    assert_eq!(
        orders
            .compact(Antichain::from_elem(4).borrow())
            .unwrap_err()
            .to_string(),
        "stash error: compact request {4} is greater than the current upper frontier {3}",
    );
    assert_eq!(
        orders
            .update(("wodgets".into(), "1".into()), 2, 1)
            .unwrap_err()
            .to_string(),
        "stash error: entry time 2 is less than the current upper frontier {3}",
    );

    // Test advancing since and upper to the empty frontier.
    orders.seal(Antichain::new().borrow())?;
    orders.compact(Antichain::new().borrow())?;
    assert_eq!(
        match orders.iter() {
            Ok(_) => panic!("call to iter unexpectedly succeeded"),
            Err(e) => e.to_string(),
        },
        "stash error: cannot iterate collection with empty since frontier",
    );
    assert_eq!(
        match orders.iter_key("wombats".into()) {
            Ok(_) => panic!("call to iter_key unexpectedly succeeded"),
            Err(e) => e.to_string(),
        },
        "stash error: cannot iterate collection with empty since frontier",
    );
    orders.consolidate()?;

    // Double check that the other collection is still untouched.
    assert_eq!(
        other.iter()?.collect::<Vec<_>>(),
        &[(("foo".into(), "bar".into()), 1, 1)],
    );
    assert_eq!(other.since()?, Antichain::from_elem(0));
    assert_eq!(other.upper()?, Antichain::from_elem(0));

    Ok(())
}
