// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use tempfile::NamedTempFile;
use timely::progress::Antichain;

use mz_stash::{
    Append, Postgres, Sqlite, Stash, StashCollection, StashError, TableTransaction, Timestamp,
    TypedCollection,
};

#[test]
fn test_stash_sqlite() -> Result<(), anyhow::Error> {
    {
        let file = NamedTempFile::new()?;
        let mut conn = Sqlite::open(file.path())?;
        test_stash(&mut conn)?;
    }
    {
        let file = NamedTempFile::new()?;
        let mut conn = Sqlite::open(file.path())?;
        test_append(&mut conn)?;
    }
    Ok(())
}

#[test]
fn test_stash_postgres() -> Result<(), anyhow::Error> {
    use postgres::{Client, NoTls};

    let connstr = match std::env::var("POSTGRES_URL") {
        Ok(s) => s,
        Err(_) => {
            println!("skipping test_stash_postgres because POSTGRES_URL is not set");
            return Ok(());
        }
    };
    fn clear(client: &mut Client) {
        client
            .batch_execute(
                "
                    DROP TABLE IF EXISTS uppers;
                    DROP TABLE IF EXISTS  sinces;
                    DROP TABLE IF EXISTS  data;
                    DROP TABLE IF EXISTS  collections;
                    DROP TABLE IF EXISTS  fence;
                ",
            )
            .unwrap();
    }
    {
        let mut client = Client::connect(&connstr, NoTls)?;
        clear(&mut client);
        let mut conn = Postgres::open(client)?;
        test_stash(&mut conn)?;
    }
    {
        let mut client = Client::connect(&connstr, NoTls)?;
        clear(&mut client);
        let mut conn = Postgres::open(client)?;
        test_append(&mut conn)?;
    }
    // Test the fence.
    {
        let mut client1 = Client::connect(&connstr, NoTls)?;
        clear(&mut client1);
        let mut conn1 = Postgres::open(client1)?;
        let mut conn2 = Postgres::open(Client::connect(&connstr, NoTls)?)?;
        assert!(match conn1.collection::<String, String>("c") {
            Err(e) => e.is_unrecoverable(),
            _ => panic!("expected error"),
        });
        let _: StashCollection<String, String> = conn2.collection("c")?;
    }

    Ok(())
}

fn test_append(stash: &mut impl Append) -> Result<(), anyhow::Error> {
    const TYPED: TypedCollection<String, String> = TypedCollection::new("typed");

    // Can't peek if since == upper.
    assert_eq!(
        TYPED.peek_one(stash).unwrap_err().to_string(),
        "stash error: collection since {-9223372036854775808} is not less than upper {-9223372036854775808}",
    );
    TYPED.upsert_key(stash, &"k1".to_string(), &"v1".to_string())?;
    assert_eq!(
        TYPED.peek_one(stash).unwrap(),
        BTreeMap::from([("k1".to_string(), "v1".to_string())])
    );
    TYPED.upsert_key(stash, &"k1".to_string(), &"v2".to_string())?;
    assert_eq!(
        TYPED.peek_one(stash).unwrap(),
        BTreeMap::from([("k1".to_string(), "v2".to_string())])
    );
    assert_eq!(
        TYPED.peek_key_one(stash, &"k1".to_string()).unwrap(),
        Some("v2".to_string()),
    );
    assert_eq!(TYPED.peek_key_one(stash, &"k2".to_string()).unwrap(), None);
    TYPED.upsert(
        stash,
        vec![
            ("k1".to_string(), "v3".to_string()),
            ("k2".to_string(), "v4".to_string()),
        ],
    )?;
    assert_eq!(
        TYPED.peek_one(stash).unwrap(),
        BTreeMap::from([
            ("k1".to_string(), "v3".to_string()),
            ("k2".to_string(), "v4".to_string())
        ])
    );

    // Test append across collections.
    let orders = stash.collection::<String, String>("orders")?;
    let other = stash.collection::<String, String>("other")?;
    // Seal so we can invalidate the upper below.
    stash.seal(other, Antichain::from_elem(1).borrow())?;
    let mut orders_batch = orders.make_batch(stash)?;
    orders.append_to_batch(&mut orders_batch, &"k1".to_string(), &"v1".to_string(), 1);
    let mut other_batch = other.make_batch(stash)?;
    other.append_to_batch(&mut other_batch, &"k2".to_string(), &"v2".to_string(), 1);

    // Invalidate one upper and ensure append doesn't commit partial batches.
    let other_upper = other_batch.upper;
    other_batch.upper = Antichain::from_elem(Timestamp::MIN);
    assert_eq!(
        stash
            .append(vec![orders_batch.clone(), other_batch.clone()])
            .unwrap_err()
            .to_string(),
        "stash error: seal request {-9223372036854775808} is less than the current upper frontier {1}",
    );
    // Test batches in the other direction too.
    assert_eq!(
        stash
            .append(vec![other_batch.clone(),orders_batch.clone() ])
            .unwrap_err()
            .to_string(),
        "stash error: seal request {-9223372036854775808} is less than the current upper frontier {1}",
    );

    // Fix the upper, append should work now.
    other_batch.upper = other_upper;
    stash.append(vec![other_batch, orders_batch])?;
    assert_eq!(
        stash.iter(orders)?,
        &[(("k1".into(), "v1".into()), -9223372036854775808, 1),]
    );
    assert_eq!(stash.iter(other)?, &[(("k2".into(), "v2".into()), 1, 1),]);
    assert_eq!(
        stash.peek_one(orders)?,
        BTreeMap::from([("k1".to_string(), "v1".to_string())])
    );
    assert_eq!(
        stash.peek_one(other)?,
        BTreeMap::from([("k2".to_string(), "v2".to_string())])
    );

    test_stash_table(stash)?;

    Ok(())
}

fn test_stash(stash: &mut impl Stash) -> Result<(), anyhow::Error> {
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
    // Peek should not observe widgets from timestamps 3 or 4.
    assert_eq!(stash.peek_timestamp(orders)?, 2);
    assert_eq!(stash.peek(orders)?, vec![("widgets".into(), "1".into(), 2)]);
    assert_eq!(
        stash.peek_one(orders).unwrap_err().to_string(),
        "stash error: unexpected peek multiplicity"
    );
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

    // Test peek_one.
    stash.seal(other, Antichain::from_elem(2).borrow())?;
    assert_eq!(
        stash.peek_one(other)?,
        BTreeMap::from([("foo".to_string(), "bar".to_string())])
    );
    assert_eq!(
        stash.peek_key_one(other, &"foo".to_string())?,
        Some("bar".to_string())
    );

    Ok(())
}

fn test_stash_table(stash: &mut impl Append) -> Result<(), anyhow::Error> {
    const TABLE: TypedCollection<Vec<u8>, String> = TypedCollection::new("table");
    fn numeric_identity(k: &Vec<u8>) -> i64 {
        i64::from_le_bytes(k.clone().try_into().unwrap())
    }
    fn uniqueness_violation(a: &String, b: &String) -> bool {
        a == b
    }
    let collection = TABLE.get(stash)?;

    fn commit(
        stash: &mut impl Append,
        collection: StashCollection<Vec<u8>, String>,
        pending: Vec<(Vec<u8>, String, i64)>,
    ) -> Result<(), StashError> {
        let mut batch = collection.make_batch(stash)?;
        for (k, v, diff) in pending {
            collection.append_to_batch(&mut batch, &k, &v, diff);
        }
        stash.append(vec![batch])?;
        Ok(())
    }

    TABLE.upsert_key(stash, &1i64.to_le_bytes().to_vec(), &"v1".to_string())?;
    TABLE.upsert(stash, vec![(2i64.to_le_bytes().to_vec(), "v2".to_string())])?;
    let mut table = TableTransaction::new(
        TABLE.peek_one(stash)?,
        Some(numeric_identity),
        uniqueness_violation,
    );
    assert_eq!(
        table.items(),
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v1".to_string()),
            (2i64.to_le_bytes().to_vec(), "v2".to_string())
        ])
    );
    assert_eq!(table.delete(|_k, _v| false), 0);
    assert_eq!(table.delete(|_k, v| v == "v2"), 1);
    assert_eq!(
        table.items(),
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v1".to_string())])
    );
    assert_eq!(table.update(|_k, _v| Some("v3".to_string()))?, 1);

    // Uniqueness violation.
    table
        .insert(|id| id.unwrap().to_le_bytes().to_vec(), "v3".to_string())
        .unwrap_err();

    assert_eq!(
        table
            .insert(|id| id.unwrap().to_le_bytes().to_vec(), "v4".to_string())
            .unwrap(),
        Some(3)
    );
    assert_eq!(
        table.items(),
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v3".to_string()),
            (3i64.to_le_bytes().to_vec(), "v4".to_string()),
        ])
    );
    assert_eq!(
        table
            .update(|_k, _v| Some("v1".to_string()))
            .unwrap_err()
            .to_string(),
        "stash error: uniqueness violation"
    );
    let pending = table.pending();
    assert_eq!(
        pending,
        vec![
            (1i64.to_le_bytes().to_vec(), "v1".to_string(), -1),
            (1i64.to_le_bytes().to_vec(), "v3".to_string(), 1),
            (2i64.to_le_bytes().to_vec(), "v2".to_string(), -1),
            (3i64.to_le_bytes().to_vec(), "v4".to_string(), 1),
        ]
    );
    commit(stash, collection, pending)?;
    let items = TABLE.peek_one(stash)?;
    assert_eq!(
        items,
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v3".to_string()),
            (3i64.to_le_bytes().to_vec(), "v4".to_string())
        ])
    );

    let mut table = TableTransaction::new(items, Some(numeric_identity), uniqueness_violation);
    // Deleting then creating an item that has a uniqueness violation should work.
    assert_eq!(table.delete(|k, _v| k == &1i64.to_le_bytes()), 1);
    table
        .insert(|_| 1i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap();
    // Uniqueness violation in value.
    table
        .insert(|_| 5i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap_err();
    // Key already exists, expect error.
    table
        .insert(|_| 1i64.to_le_bytes().to_vec(), "v5".to_string())
        .unwrap_err();
    assert_eq!(table.delete(|k, _v| k == &1i64.to_le_bytes()), 1);
    // Both the inserts work now because the key and uniqueness violation are gone.
    table
        .insert(|_| 5i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap();
    table
        .insert(|_| 1i64.to_le_bytes().to_vec(), "v5".to_string())
        .unwrap();
    let pending = table.pending();
    assert_eq!(
        pending,
        vec![
            (1i64.to_le_bytes().to_vec(), "v3".to_string(), -1),
            (1i64.to_le_bytes().to_vec(), "v5".to_string(), 1),
            (5i64.to_le_bytes().to_vec(), "v3".to_string(), 1),
        ]
    );
    commit(stash, collection, pending)?;
    let items = TABLE.peek_one(stash)?;
    assert_eq!(
        items,
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v5".to_string()),
            (3i64.to_le_bytes().to_vec(), "v4".to_string()),
            (5i64.to_le_bytes().to_vec(), "v3".to_string()),
        ])
    );

    let mut table = TableTransaction::new(items, Some(numeric_identity), uniqueness_violation);
    assert_eq!(table.delete(|_k, _v| true), 3);
    table
        .insert(|_| 1i64.to_le_bytes().to_vec(), "v1".to_string())
        .unwrap();

    commit(stash, collection, table.pending())?;
    let items = TABLE.peek_one(stash)?;
    assert_eq!(
        items,
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v1".to_string()),])
    );

    let mut table = TableTransaction::new(items, Some(numeric_identity), uniqueness_violation);
    assert_eq!(table.delete(|_k, _v| true), 1);
    table
        .insert(|_| 1i64.to_le_bytes().to_vec(), "v2".to_string())
        .unwrap();
    commit(stash, collection, table.pending())?;
    let items = TABLE.peek_one(stash)?;
    assert_eq!(
        items,
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v2".to_string()),])
    );

    // Verify we don't try to delete v3 or v4 during commit.
    let mut table = TableTransaction::new(items, Some(numeric_identity), uniqueness_violation);
    assert_eq!(table.delete(|_k, _v| true), 1);
    table
        .insert(|_| 1i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap();
    table
        .insert(|_| 1i64.to_le_bytes().to_vec(), "v4".to_string())
        .unwrap_err();
    assert_eq!(table.delete(|_k, _v| true), 1);
    table
        .insert(|_| 1i64.to_le_bytes().to_vec(), "v5".to_string())
        .unwrap();
    commit(stash, collection, table.pending())?;
    let items = stash.peek(collection)?;
    assert_eq!(
        items,
        vec![(1i64.to_le_bytes().to_vec(), "v5".to_string(), 1)]
    );

    Ok(())
}

#[test]
fn test_table() {
    fn numeric_identity(k: &Vec<u8>) -> i64 {
        i64::from_le_bytes(k.clone().try_into().unwrap())
    }
    fn uniqueness_violation(a: &String, b: &String) -> bool {
        a == b
    }
    let mut table = TableTransaction::new(
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "a".to_string())]),
        Some(numeric_identity),
        uniqueness_violation,
    );
    // Test the auto-increment id.
    assert_eq!(
        table
            .insert(|id| id.unwrap().to_le_bytes().to_vec(), "b".to_string())
            .unwrap(),
        Some(2)
    );
    assert_eq!(
        table
            .insert(|id| id.unwrap().to_le_bytes().to_vec(), "c".to_string())
            .unwrap(),
        Some(3)
    );
}
