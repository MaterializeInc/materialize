// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use futures::Future;
use postgres_openssl::MakeTlsConnector;
use tempfile::NamedTempFile;
use tokio_postgres::Config;

use mz_stash::{
    Append, Diff, Memory, Postgres, Sqlite, Stash, StashCollection, StashError, TableTransaction,
    TypedCollection,
};

#[tokio::test]
async fn test_stash_memory() -> Result<(), anyhow::Error> {
    {
        let file = NamedTempFile::new()?;
        test_stash(|| async { Memory::new(Sqlite::open(Some(file.path())).unwrap()) }).await?;
    }
    {
        let file = NamedTempFile::new()?;
        test_append(|| async { Memory::new(Sqlite::open(Some(file.path())).unwrap()) }).await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_stash_sqlite() -> Result<(), anyhow::Error> {
    {
        let file = NamedTempFile::new()?;
        test_stash(|| async { Sqlite::open(Some(file.path())).unwrap() }).await?;
    }
    {
        let file = NamedTempFile::new()?;
        test_append(|| async { Sqlite::open(Some(file.path())).unwrap() }).await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_stash_postgres() -> Result<(), anyhow::Error> {
    let tls = mz_postgres_util::make_tls(&Config::new()).unwrap();

    {
        // Verify invalid URLs fail on connect.
        assert!(Postgres::new("host=invalid".into(), None, tls.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("stash error: postgres: error connecting to server"));
    }

    let connstr = match std::env::var("POSTGRES_URL") {
        Ok(s) => s,
        Err(_) => {
            println!("skipping test_stash_postgres because POSTGRES_URL is not set");
            return Ok(());
        }
    };
    async fn connect(connstr: &str, tls: MakeTlsConnector, clear: bool) -> Postgres {
        let (client, connection) = tokio_postgres::connect(&connstr, tokio_postgres::NoTls)
            .await
            .unwrap();
        mz_ore::task::spawn(|| "postgres connection", async move {
            if let Err(e) = connection.await {
                panic!("connection error: {}", e);
            }
        });
        if clear {
            client
                .batch_execute(
                    "
                    DROP TABLE IF EXISTS  data;
                    DROP TABLE IF EXISTS  collections;
                    DROP TABLE IF EXISTS  fence;
                ",
                )
                .await
                .unwrap();
        }
        Postgres::new(connstr.to_string(), None, tls).await.unwrap()
    }
    {
        connect(&connstr, tls.clone(), true).await;
        test_stash(|| async { connect(&connstr, tls.clone(), false).await }).await?;
    }
    {
        connect(&connstr, tls.clone(), true).await;
        test_append(|| async { connect(&connstr, tls.clone(), false).await }).await?;
    }
    // Test the fence.
    {
        let mut conn1 = connect(&connstr, tls.clone(), true).await;
        // Don't clear the stash tables.
        let mut conn2 = connect(&connstr, tls.clone(), false).await;
        assert!(match conn1.collection::<String, String>("c").await {
            Err(e) => e.is_unrecoverable(),
            _ => panic!("expected error"),
        });
        let _: StashCollection<String, String> = conn2.collection("c").await?;
    }

    Ok(())
}

async fn test_append<F, S, O>(f: F) -> Result<(), anyhow::Error>
where
    S: Append,
    O: Future<Output = S>,
    F: Fn() -> O,
{
    const TYPED: TypedCollection<String, String> = TypedCollection::new("typed");

    let mut stash = f().await;

    TYPED
        .upsert_key(&mut stash, &"k1".to_string(), &"v1".to_string())
        .await?;
    assert_eq!(
        TYPED.iter(&mut stash).await.unwrap(),
        BTreeMap::from([("k1".to_string(), "v1".to_string())])
    );
    TYPED
        .upsert_key(&mut stash, &"k1".to_string(), &"v2".to_string())
        .await?;
    assert_eq!(
        TYPED.iter(&mut stash).await.unwrap(),
        BTreeMap::from([("k1".to_string(), "v2".to_string())])
    );
    assert_eq!(
        TYPED.get_key(&mut stash, &"k1".to_string()).await.unwrap(),
        Some("v2".to_string()),
    );
    assert_eq!(
        TYPED.get_key(&mut stash, &"k2".to_string()).await.unwrap(),
        None
    );
    TYPED
        .upsert(
            &mut stash,
            vec![
                ("k1".to_string(), "v3".to_string()),
                ("k2".to_string(), "v4".to_string()),
            ],
        )
        .await?;
    assert_eq!(
        TYPED.iter(&mut stash).await.unwrap(),
        BTreeMap::from([
            ("k1".to_string(), "v3".to_string()),
            ("k2".to_string(), "v4".to_string())
        ])
    );

    // Test append across collections.
    let orders = stash.collection::<String, String>("orders").await?;
    let other = stash.collection::<String, String>("other").await?;
    let mut orders_batch = orders.make_batch().await?;
    orders.append_to_batch(
        &mut orders_batch,
        &"k1".to_string(),
        &"v1".to_string(),
        Diff::Insert,
    );
    let mut other_batch = other.make_batch().await?;
    other.append_to_batch(
        &mut other_batch,
        &"k2".to_string(),
        &"v2".to_string(),
        Diff::Insert,
    );

    stash.append(vec![other_batch, orders_batch]).await?;
    assert_eq!(
        stash.iter(orders).await?,
        BTreeMap::from([("k1".into(), "v1".into())])
    );
    assert_eq!(
        stash.iter(other).await?,
        BTreeMap::from([("k2".into(), "v2".into())])
    );

    // Multiple empty batches should bump the upper and the since because append
    // must also compact and consolidate.
    for _ in 0..5 {
        let orders_batch = orders.make_batch().await?;
        stash.append(vec![orders_batch]).await?;
    }

    // Remake the stash and ensure data remains.
    let mut stash = f().await;
    assert_eq!(
        stash.iter(orders).await?,
        BTreeMap::from([("k1".to_string(), "v1".to_string())])
    );
    assert_eq!(
        stash.iter(other).await?,
        BTreeMap::from([("k2".to_string(), "v2".to_string())])
    );
    // Remake again, mutate before reading, then read.
    let mut stash = f().await;
    stash
        .update_many(orders, [(("k3".into(), "v3".into()), Diff::Insert)])
        .await?;

    assert_eq!(
        stash.iter(orders).await?,
        BTreeMap::from([
            ("k1".to_string(), "v1".to_string()),
            ("k3".to_string(), "v3".to_string())
        ])
    );

    // Remake the stash, mutate, then read.
    let mut stash = f().await;
    let mut orders_batch = orders.make_batch().await?;
    orders.append_to_batch(
        &mut orders_batch,
        &"k4".to_string(),
        &"v4".to_string(),
        Diff::Insert,
    );
    stash.append(vec![orders_batch]).await?;
    assert_eq!(
        stash.iter(orders).await?,
        BTreeMap::from([
            ("k1".to_string(), "v1".to_string()),
            ("k3".to_string(), "v3".to_string()),
            ("k4".to_string(), "v4".to_string())
        ])
    );

    // Remake and read again.
    let mut stash = f().await;
    assert_eq!(
        stash.iter(orders).await?,
        BTreeMap::from([
            ("k1".to_string(), "v1".to_string()),
            ("k3".to_string(), "v3".to_string()),
            ("k4".to_string(), "v4".to_string())
        ])
    );

    test_stash_table(&mut stash).await?;

    Ok(())
}

async fn test_stash<F, S, O>(f: F) -> Result<(), anyhow::Error>
where
    S: Stash,
    O: Future<Output = S>,
    F: Fn() -> O,
{
    let mut stash = f().await;
    // Create an arrangement, write some data into it, then read it back.
    let orders = stash.collection::<String, String>("orders").await?;
    stash
        .update(orders, ("widgets".into(), "1".into()), Diff::Insert)
        .await?;
    stash
        .update(orders, ("wombats".into(), "2".into()), Diff::Insert)
        .await?;
    assert_eq!(
        stash
            .update(orders, ("wombats".into(), "2".into()), Diff::Insert)
            .await
            .unwrap_err()
            .to_string(),
        "stash error: cannot insert with an existing key",
    );
    assert_eq!(
        stash.iter(orders).await?,
        BTreeMap::from([
            ("widgets".into(), "1".into()),
            ("wombats".into(), "2".into()),
        ])
    );
    assert_eq!(
        stash.get_key(orders, &"widgets".to_string()).await?,
        Some("1".into())
    );
    assert_eq!(
        stash.get_key(orders, &"wombats".to_string()).await?,
        Some("2".into())
    );

    // Write to another arrangement and ensure the data stays separate.
    let other = stash.collection::<String, String>("other").await?;
    stash
        .update(other, ("foo".into(), "bar".into()), Diff::Insert)
        .await?;
    assert_eq!(
        stash.iter(other).await?,
        BTreeMap::from([("foo".into(), "bar".into())]),
    );
    assert_eq!(
        stash.iter(orders).await?,
        BTreeMap::from([
            ("widgets".into(), "1".into()),
            ("wombats".into(), "2".into()),
        ])
    );

    stash
        .update(orders, ("wombats".into(), "2".into()), Diff::Delete)
        .await?;
    assert_eq!(
        stash.iter(orders).await?,
        BTreeMap::from([("widgets".into(), "1".into()),])
    );

    assert_eq!(
        stash
            .update(orders, ("wombats".into(), "2".into()), Diff::Delete)
            .await
            .unwrap_err()
            .to_string(),
        "stash error: cannot delete non-existent key"
    );

    Ok(())
}

async fn test_stash_table(stash: &mut impl Append) -> Result<(), anyhow::Error> {
    const TABLE: TypedCollection<Vec<u8>, String> = TypedCollection::new("table");
    fn uniqueness_violation(a: &String, b: &String) -> bool {
        a == b
    }
    let collection = TABLE.get(stash).await?;

    async fn commit(
        stash: &mut impl Append,
        collection: StashCollection<Vec<u8>, String>,
        pending: Vec<(Vec<u8>, String, Diff)>,
    ) -> Result<(), StashError> {
        let mut batch = collection.make_batch().await?;
        for (k, v, diff) in pending {
            collection.append_to_batch(&mut batch, &k, &v, diff);
        }
        stash.append(vec![batch]).await?;
        Ok(())
    }

    TABLE
        .upsert_key(stash, &1i64.to_le_bytes().to_vec(), &"v1".to_string())
        .await?;
    TABLE
        .upsert(stash, vec![(2i64.to_le_bytes().to_vec(), "v2".to_string())])
        .await?;
    let mut table = TableTransaction::new(TABLE.iter(stash).await?, uniqueness_violation);
    assert_eq!(
        table.items(),
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v1".to_string()),
            (2i64.to_le_bytes().to_vec(), "v2".to_string())
        ])
    );
    assert_eq!(table.delete(|_k, _v| false).len(), 0);
    assert_eq!(table.delete(|_k, v| v == "v2").len(), 1);
    assert_eq!(
        table.items(),
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v1".to_string())])
    );
    assert_eq!(table.update(|_k, _v| Some("v3".to_string()))?, 1);

    // Uniqueness violation.
    table
        .insert(3i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap_err();

    table
        .insert(3i64.to_le_bytes().to_vec(), "v4".to_string())
        .unwrap();
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
            (1i64.to_le_bytes().to_vec(), "v1".to_string(), Diff::Delete),
            (1i64.to_le_bytes().to_vec(), "v3".to_string(), Diff::Insert),
            (2i64.to_le_bytes().to_vec(), "v2".to_string(), Diff::Delete),
            (3i64.to_le_bytes().to_vec(), "v4".to_string(), Diff::Insert),
        ]
    );
    commit(stash, collection, pending).await?;
    let items = TABLE.iter(stash).await?;
    assert_eq!(
        items,
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v3".to_string()),
            (3i64.to_le_bytes().to_vec(), "v4".to_string())
        ])
    );

    let mut table = TableTransaction::new(items, uniqueness_violation);
    // Deleting then creating an item that has a uniqueness violation should work.
    assert_eq!(table.delete(|k, _v| k == &1i64.to_le_bytes()).len(), 1);
    table
        .insert(1i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap();
    // Uniqueness violation in value.
    table
        .insert(5i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap_err();
    // Key already exists, expect error.
    table
        .insert(1i64.to_le_bytes().to_vec(), "v5".to_string())
        .unwrap_err();
    assert_eq!(table.delete(|k, _v| k == &1i64.to_le_bytes()).len(), 1);
    // Both the inserts work now because the key and uniqueness violation are gone.
    table
        .insert(5i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap();
    table
        .insert(1i64.to_le_bytes().to_vec(), "v5".to_string())
        .unwrap();
    let pending = table.pending();
    assert_eq!(
        pending,
        vec![
            (1i64.to_le_bytes().to_vec(), "v3".to_string(), Diff::Delete),
            (1i64.to_le_bytes().to_vec(), "v5".to_string(), Diff::Insert),
            (5i64.to_le_bytes().to_vec(), "v3".to_string(), Diff::Insert),
        ]
    );
    commit(stash, collection, pending).await?;
    let items = TABLE.iter(stash).await?;
    assert_eq!(
        items,
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v5".to_string()),
            (3i64.to_le_bytes().to_vec(), "v4".to_string()),
            (5i64.to_le_bytes().to_vec(), "v3".to_string()),
        ])
    );

    let mut table = TableTransaction::new(items, uniqueness_violation);
    assert_eq!(table.delete(|_k, _v| true).len(), 3);
    table
        .insert(1i64.to_le_bytes().to_vec(), "v1".to_string())
        .unwrap();

    commit(stash, collection, table.pending()).await?;
    let items = TABLE.iter(stash).await?;
    assert_eq!(
        items,
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v1".to_string()),])
    );

    let mut table = TableTransaction::new(items, uniqueness_violation);
    assert_eq!(table.delete(|_k, _v| true).len(), 1);
    table
        .insert(1i64.to_le_bytes().to_vec(), "v2".to_string())
        .unwrap();
    commit(stash, collection, table.pending()).await?;
    let items = TABLE.iter(stash).await?;
    assert_eq!(
        items,
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v2".to_string()),])
    );

    // Verify we don't try to delete v3 or v4 during commit.
    let mut table = TableTransaction::new(items, uniqueness_violation);
    assert_eq!(table.delete(|_k, _v| true).len(), 1);
    table
        .insert(1i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap();
    table
        .insert(1i64.to_le_bytes().to_vec(), "v4".to_string())
        .unwrap_err();
    assert_eq!(table.delete(|_k, _v| true).len(), 1);
    table
        .insert(1i64.to_le_bytes().to_vec(), "v5".to_string())
        .unwrap();
    commit(stash, collection, table.pending()).await?;
    let items = stash.iter(collection).await?;
    assert_eq!(
        items,
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v5".to_string())])
    );

    Ok(())
}

#[test]
fn test_table() {
    fn uniqueness_violation(a: &String, b: &String) -> bool {
        a == b
    }
    let mut table = TableTransaction::new(
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "a".to_string())]),
        uniqueness_violation,
    );

    table
        .insert(2i64.to_le_bytes().to_vec(), "b".to_string())
        .unwrap();
    table
        .insert(3i64.to_le_bytes().to_vec(), "c".to_string())
        .unwrap();
}
