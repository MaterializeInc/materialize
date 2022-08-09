// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Durable metadata storage.

use std::collections::HashSet;
use std::marker::PhantomData;
use std::{cmp, time::Duration};

use async_trait::async_trait;
use futures::future::{self, try_join3, try_join_all, BoxFuture};
use futures::future::{try_join, TryFutureExt};
use futures::StreamExt;
use postgres_openssl::MakeTlsConnector;
use timely::progress::frontier::AntichainRef;
use timely::progress::Antichain;
use timely::PartialOrder;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Statement, Transaction};
use tracing::warn;

use mz_ore::retry::Retry;

use crate::{
    AntichainFormatter, Append, AppendBatch, Data, Diff, Id, InternalStashError, Stash,
    StashCollection, StashError, Timestamp,
};

const SCHEMA: &str = "
CREATE TABLE fence (
    epoch bigint PRIMARY KEY,
    nonce bytea
);
INSERT INTO fence VALUES (1, '');

-- bigserial is not ideal for Cockroach, but we have a stable number of
-- collections, so our use of it here is fine and compatible with Postgres.
CREATE TABLE collections (
    collection_id bigserial PRIMARY KEY,
    name text NOT NULL UNIQUE
);

CREATE TABLE data (
    collection_id bigint NOT NULL REFERENCES collections (collection_id),
    key bytea NOT NULL,
    value bytea NOT NULL,
    time bigint NOT NULL,
    diff bigint NOT NULL
);

CREATE INDEX data_time_idx ON data (collection_id, time);

CREATE TABLE sinces (
    collection_id bigint PRIMARY KEY REFERENCES collections (collection_id),
    since bigint
);

CREATE TABLE uppers (
    collection_id bigint PRIMARY KEY REFERENCES collections (collection_id),
    upper bigint
);
";

struct PreparedStatements {
    select_epoch: Statement,
    iter_key: Statement,
    since: Statement,
    upper: Statement,
    collection: Statement,
    iter: Statement,
    seal: Statement,
    compact: Statement,
    consolidate_consolidation: Statement,
    consolidate_insert: Statement,
    consolidate_delete: Statement,
    update_many: Statement,
}

/// A Stash whose data is stored in a Postgres database. The format of the
/// tables are not specified and should not be relied upon. The only promise is
/// stability. Any changes to the table schemas will be accompanied by a clear
/// migration path.
pub struct Postgres {
    url: String,
    schema: Option<String>,
    tls: MakeTlsConnector,
    client: Option<Client>,
    statements: Option<PreparedStatements>,
    epoch: Option<i64>,
    nonce: [u8; 16],
}

impl std::fmt::Debug for Postgres {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Postgres")
            .field("url", &self.url)
            .field("epoch", &self.epoch)
            .field("nonce", &self.nonce)
            .finish_non_exhaustive()
    }
}

impl Postgres {
    /// Opens the stash stored at the specified path.
    pub async fn new(
        url: String,
        schema: Option<String>,
        tls: MakeTlsConnector,
    ) -> Result<Postgres, StashError> {
        let mut conn = Postgres {
            url,
            schema,
            tls,
            client: None,
            statements: None,
            epoch: None,
            // The call to rand::random here assumes that the seed source is from a secure
            // source that will differ per thread. The docs for ThreadRng say it "is
            // automatically seeded from OsRng", which meets this requirement.
            nonce: rand::random(),
        };
        // Do the initial connection once here so we don't get stuck in transact's
        // retry loop if the url is bad.
        conn.connect().await?;
        Ok(conn)
    }

    /// Sets `client` to a new connection to the Postgres server.
    async fn connect(&mut self) -> Result<(), StashError> {
        let (mut client, connection) = tokio_postgres::connect(&self.url, self.tls.clone()).await?;
        mz_ore::task::spawn(|| "tokio-postgres stash connection", async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres stash connection error: {}", e);
            }
        });
        client
            .batch_execute("SET default_transaction_isolation = serializable")
            .await?;
        if let Some(schema) = &self.schema {
            client
                .execute(format!("SET search_path TO {schema}").as_str(), &[])
                .await?;
        }

        if self.epoch.is_none() {
            let tx = client.transaction().await?;
            let fence_exists: bool = tx
                .query_one(
                    r#"
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = current_schema() AND tablename = 'fence'
            )"#,
                    &[],
                )
                .await?
                .get(0);
            if !fence_exists {
                tx.batch_execute(SCHEMA).await?;
            }
            // Bump the epoch, which will cause any previous connection to fail. Add a
            // unique nonce so that if some other thing recreates the entire schema, we
            // can't accidentally have the same epoch, nonce pair (especially risky if the
            // current epoch has been bumped exactly once, then gets recreated by another
            // connection that also bumps it once).
            let epoch = tx
                .query_one(
                    "UPDATE fence SET epoch=epoch+1, nonce=$1 RETURNING epoch",
                    &[&self.nonce.to_vec()],
                )
                .await?
                .get(0);
            tx.commit().await?;
            self.epoch = Some(epoch);
        }

        let select_epoch = client.prepare("SELECT epoch, nonce FROM fence").await?;
        let iter_key = client
            .prepare(
                "SELECT value, time, diff FROM data
                 WHERE collection_id = $1 AND key = $2",
            )
            .await?;
        let since = client
            .prepare("SELECT since FROM sinces WHERE collection_id = $1")
            .await?;
        let upper = client
            .prepare("SELECT upper FROM uppers WHERE collection_id = $1")
            .await?;
        let collection = client
            .prepare("SELECT collection_id FROM collections WHERE name = $1")
            .await?;
        let iter = client
            .prepare(
                "SELECT key, value, time, diff FROM data
                 WHERE collection_id = $1",
            )
            .await?;
        let seal = client
            .prepare("UPDATE uppers SET upper = $1 WHERE collection_id = $2")
            .await?;
        let compact = client
            .prepare("UPDATE sinces SET since = $1 WHERE collection_id = $2")
            .await?;
        let consolidate_consolidation = client
            .prepare(
                "DELETE FROM data
                 WHERE collection_id = $1 AND time <= $2
                 RETURNING key, value, diff",
            )
            .await?;
        let consolidate_insert = client
            .prepare(
                "INSERT INTO data (collection_id, key, value, time, diff)
                VALUES ($1, $2, $3, $4, $5)",
            )
            .await?;
        let consolidate_delete = client
            .prepare("DELETE FROM data WHERE collection_id = $1")
            .await?;
        let update_many = client
            .prepare(
                "INSERT INTO data (collection_id, key, value, time, diff)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .await?;
        self.client = Some(client);
        self.statements = Some(PreparedStatements {
            select_epoch,
            iter_key,
            since,
            upper,
            collection,
            iter,
            seal,
            compact,
            consolidate_consolidation,
            consolidate_insert,
            consolidate_delete,
            update_many,
        });
        Ok(())
    }

    /// Construct a fenced transaction, which will cause this Stash to fail if
    /// another connection is opened to it. `f` may be called multiple times in a
    /// backoff-retry loop if the Postgres server is unavailable, so it should only
    /// call functions on its Transaction argument.
    ///
    /// # Examples
    ///
    /// ```text
    /// async fn x(&mut self) -> Result<(), StashError> {
    ///     self.transact(move |stmts, tx| {
    ///         Box::pin(async move {
    ///             // Use tx.
    ///         })
    ///     })
    ///     .await
    //  }
    /// ```
    #[tracing::instrument(level = "debug", skip_all)]
    async fn transact<F, T>(&mut self, f: F) -> Result<T, StashError>
    where
        F: for<'a> Fn(
            &'a PreparedStatements,
            &'a Transaction,
        ) -> BoxFuture<'a, Result<T, StashError>>,
    {
        let retry = Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .into_retry_stream();
        let mut retry = Box::pin(retry);
        let mut attempt: u64 = 0;
        loop {
            match self.transact_inner(&f).await {
                Ok(r) => return Ok(r),
                Err(e) => match &e.inner {
                    InternalStashError::Postgres(pgerr) => {
                        // Some errors aren't retryable.
                        if let Some(dberr) = pgerr.as_db_error() {
                            if dberr.code() == &SqlState::UNDEFINED_TABLE {
                                return Err(e);
                            }
                        }
                        attempt += 1;
                        warn!("tokio-postgres stash error, retry attempt {attempt}: {pgerr}");
                        self.client = None;
                        retry.next().await;
                    }
                    _ => return Err(e),
                },
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn transact_inner<F, T>(&mut self, f: &F) -> Result<T, StashError>
    where
        F: for<'a> Fn(
            &'a PreparedStatements,
            &'a Transaction,
        ) -> BoxFuture<'a, Result<T, StashError>>,
    {
        let reconnect = match &self.client {
            Some(client) => client.is_closed(),
            None => true,
        };
        if reconnect {
            self.connect().await?;
        }
        // client is guaranteed to be Some here.
        let client = self.client.as_mut().unwrap();
        let stmts = self.statements.as_ref().unwrap();
        let tx = client.transaction().await?;
        // Pipeline the epoch query and closure.
        let epoch_fut = tx
            .query_one(&stmts.select_epoch, &[])
            .map_err(|err| err.into());
        let f_fut = f(stmts, &tx);
        let (row, res) = future::try_join(epoch_fut, f_fut).await?;
        let current_epoch: i64 = row.get(0);
        if Some(current_epoch) != self.epoch {
            return Err(InternalStashError::Fence(format!(
                "unexpected fence epoch {}, expected {:?}",
                current_epoch, self.epoch
            ))
            .into());
        }
        let current_nonce: Vec<u8> = row.get(1);
        if current_nonce != self.nonce {
            return Err(InternalStashError::Fence("unexpected fence nonce".into()).into());
        }
        tx.commit().await?;
        Ok(res)
    }

    async fn since_tx(
        stmts: &PreparedStatements,
        tx: &Transaction<'_>,
        collection_id: Id,
    ) -> Result<Antichain<Timestamp>, StashError> {
        let since: Option<Timestamp> = tx
            .query_one(&stmts.since, &[&collection_id])
            .await?
            .get("since");
        Ok(Antichain::from_iter(since))
    }

    async fn upper_tx(
        stmts: &PreparedStatements,
        tx: &Transaction<'_>,
        collection_id: Id,
    ) -> Result<Antichain<Timestamp>, StashError> {
        let upper: Option<Timestamp> = tx
            .query_one(&stmts.upper, &[&collection_id])
            .await?
            .get("upper");
        Ok(Antichain::from_iter(upper))
    }

    async fn seal_batch_tx<'a, I>(
        stmts: &PreparedStatements,
        tx: &Transaction<'_>,
        seals: I,
    ) -> Result<(), StashError>
    where
        I: Iterator<Item = (Id, &'a Antichain<Timestamp>)>,
    {
        for (collection_id, new_upper) in seals {
            let upper = Self::upper_tx(stmts, tx, collection_id).await?;
            if PartialOrder::less_than(new_upper, &upper) {
                return Err(StashError::from(format!(
                    "seal request {} is less than the current upper frontier {}",
                    AntichainFormatter(new_upper),
                    AntichainFormatter(&upper),
                )));
            }
            tx.execute(&stmts.seal, &[&new_upper.as_option(), &collection_id])
                .await?;
        }
        Ok(())
    }

    /// Adds entries to the collection. Returns the collection's upper.
    async fn update_many_tx(
        stmts: &PreparedStatements,
        tx: &Transaction<'_>,
        collection_id: Id,
        entries: &[((Vec<u8>, Vec<u8>), Timestamp, Diff)],
    ) -> Result<Antichain<Timestamp>, StashError> {
        let mut futures = Vec::with_capacity(entries.len());
        for ((key, value), time, diff) in entries {
            futures.push(async move {
                tx.execute(
                    &stmts.update_many,
                    &[&collection_id, &key, &value, &time, &diff],
                )
                .map_err(|err| err.into())
                .await
            });
        }
        // Check the upper in a separate future so we can issue the updates without
        // waiting for it first.
        let (upper, _) = try_join(
            async {
                let upper = Self::upper_tx(stmts, tx, collection_id).await?;
                for ((_key, _value), time, _diff) in entries {
                    if !upper.less_equal(&time) {
                        return Err(StashError::from(format!(
                            "entry time {} is less than the current upper frontier {}",
                            time,
                            AntichainFormatter(&upper)
                        )));
                    }
                }
                Ok(upper)
            },
            try_join_all(futures),
        )
        .await?;
        Ok(upper)
    }

    async fn compact_batch_tx<'a, I>(
        stmts: &PreparedStatements,
        tx: &Transaction<'_>,
        compactions: I,
    ) -> Result<(), StashError>
    where
        I: Iterator<Item = (Id, &'a Antichain<Timestamp>)>,
    {
        let futures = compactions.map(|(collection_id, new_since)| {
            try_join3(
                async move {
                    let since = Self::since_tx(stmts, tx, collection_id).await?;
                    if PartialOrder::less_than(new_since, &since) {
                        return Err(StashError::from(format!(
                            "compact request {} is less than the current since frontier {}",
                            AntichainFormatter(new_since),
                            AntichainFormatter(&since)
                        )));
                    }
                    Ok(())
                },
                async move {
                    let upper = Self::upper_tx(stmts, tx, collection_id).await?;
                    if PartialOrder::less_than(&upper, new_since) {
                        return Err(StashError::from(format!(
                            "compact request {} is greater than the current upper frontier {}",
                            AntichainFormatter(new_since),
                            AntichainFormatter(&upper)
                        )));
                    }
                    Ok(())
                },
                async move {
                    tx.execute(&stmts.compact, &[&new_since.as_option(), &collection_id])
                        .map_err(StashError::from)
                        .await
                },
            )
        });
        try_join_all(futures).await?;
        Ok(())
    }

    async fn consolidate_batch_tx<I>(
        stmts: &PreparedStatements,
        tx: &Transaction<'_>,
        collections: I,
    ) -> Result<(), StashError>
    where
        I: Iterator<Item = Id>,
    {
        for collection_id in collections {
            let since = Self::since_tx(stmts, tx, collection_id)
                .await?
                .into_option();
            match since {
                Some(since) => {
                    let mut updates = tx
                        .query(&stmts.consolidate_consolidation, &[&collection_id, &since])
                        .await?
                        .into_iter()
                        .map(|row| {
                            let key = row.try_get("key")?;
                            let value = row.try_get("value")?;
                            let diff = row.try_get("diff")?;
                            Ok::<_, StashError>(((key, value), since, diff))
                        })
                        .collect::<Result<Vec<((Vec<u8>, Vec<u8>), i64, i64)>, _>>()?;
                    differential_dataflow::consolidation::consolidate_updates(&mut updates);
                    for ((key, value), time, diff) in updates {
                        tx.execute(
                            &stmts.consolidate_insert,
                            &[&collection_id, &key, &value, &time, &diff],
                        )
                        .await?;
                    }
                }
                None => {
                    tx.execute(&stmts.consolidate_delete, &[&collection_id])
                        .await?;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Stash for Postgres {
    async fn collection<K, V>(&mut self, name: &str) -> Result<StashCollection<K, V>, StashError>
    where
        K: Data,
        V: Data,
    {
        let name = name.to_string();
        self.transact(move |stmts, tx| {
            let name = name.clone();
            Box::pin(async move {
                let collection_id_opt: Option<_> = tx
                    .query_one(&stmts.collection, &[&name])
                    .await
                    .map(|row| row.get("collection_id"))
                    .ok();

                let collection_id = match collection_id_opt {
                    Some(id) => id,
                    None => {
                        let collection_id = tx
                        .query_one(
                            "INSERT INTO collections (name) VALUES ($1) RETURNING collection_id",
                            &[&name],
                        )
                        .await?
                        .get("collection_id");
                        tx.execute(
                            "INSERT INTO sinces (collection_id, since) VALUES ($1, $2)",
                            &[&collection_id, &Timestamp::MIN],
                        )
                        .await?;
                        tx.execute(
                            "INSERT INTO uppers (collection_id, upper) VALUES ($1, $2)",
                            &[&collection_id, &Timestamp::MIN],
                        )
                        .await?;
                        collection_id
                    }
                };

                Ok(StashCollection {
                    id: collection_id,
                    _kv: PhantomData,
                })
            })
        })
        .await
    }

    async fn iter<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Vec<((K, V), Timestamp, Diff)>, StashError>
    where
        K: Data,
        V: Data,
    {
        self.transact(move |stmts, tx| {
            Box::pin(async move {
                let since = match Self::since_tx(stmts, tx, collection.id)
                    .await?
                    .into_option()
                {
                    Some(since) => since,
                    None => {
                        return Err(StashError::from(
                            "cannot iterate collection with empty since frontier",
                        ));
                    }
                };
                let mut rows = tx
                    .query(&stmts.iter, &[&collection.id])
                    .await?
                    .into_iter()
                    .map(|row| {
                        let key_buf: Vec<_> = row.try_get("key")?;
                        let value_buf: Vec<_> = row.try_get("value")?;
                        let key = K::decode(&key_buf)?;
                        let value = V::decode(&value_buf)?;
                        let time = row.try_get("time")?;
                        let diff = row.try_get("diff")?;
                        Ok::<_, StashError>(((key, value), cmp::max(time, since), diff))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                differential_dataflow::consolidation::consolidate_updates(&mut rows);
                Ok(rows)
            })
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn iter_key<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Vec<(V, Timestamp, Diff)>, StashError>
    where
        K: Data,
        V: Data,
    {
        let mut key_buf = vec![];
        key.encode(&mut key_buf);
        self.transact(move |stmts, tx| {
            let key_buf = key_buf.clone();
            Box::pin(async move {
                let (since, rows) = future::try_join(
                    Self::since_tx(stmts, tx, collection.id),
                    tx.query(&stmts.iter_key, &[&collection.id, &key_buf])
                        .map_err(|err| err.into()),
                )
                .await?;
                let since = match since.into_option() {
                    Some(since) => since,
                    None => {
                        return Err(StashError::from(
                            "cannot iterate collection with empty since frontier",
                        ));
                    }
                };
                let mut rows = rows
                    .into_iter()
                    .map(|row| {
                        let value_buf: Vec<_> = row.try_get("value")?;
                        let value = V::decode(&value_buf)?;
                        let time = row.try_get("time")?;
                        let diff = row.try_get("diff")?;
                        Ok::<_, StashError>((value, cmp::max(time, since), diff))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                differential_dataflow::consolidation::consolidate_updates(&mut rows);
                Ok(rows)
            })
        })
        .await
    }

    async fn update_many<K, V, I>(
        &mut self,
        collection: StashCollection<K, V>,
        entries: I,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
        I: IntoIterator<Item = ((K, V), Timestamp, Diff)> + Send,
        I::IntoIter: Send,
    {
        let entries = entries
            .into_iter()
            .map(|((key, value), time, diff)| {
                let mut key_buf = vec![];
                let mut value_buf = vec![];
                key.encode(&mut key_buf);
                value.encode(&mut value_buf);
                ((key_buf, value_buf), time, diff)
            })
            .collect::<Vec<_>>();

        self.transact(move |stmts, tx| {
            let entries = entries.clone();
            Box::pin(async move {
                Self::update_many_tx(stmts, tx, collection.id, &entries).await?;
                Ok(())
            })
        })
        .await
    }

    async fn seal<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        new_upper: AntichainRef<'_, Timestamp>,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.seal_batch(&[(collection, new_upper.to_owned())]).await
    }

    async fn seal_batch<K, V>(
        &mut self,
        seals: &[(StashCollection<K, V>, Antichain<Timestamp>)],
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        let seals = seals
            .iter()
            .map(|(collection, frontier)| (collection.id, frontier.clone()))
            .collect::<Vec<_>>();
        self.transact(move |stmts, tx| {
            let seals = seals.clone();
            Box::pin(async move {
                Self::seal_batch_tx(stmts, tx, seals.iter().map(|d| (d.0, &d.1))).await
            })
        })
        .await
    }

    async fn compact<'a, K, V>(
        &'a mut self,
        collection: StashCollection<K, V>,
        new_since: AntichainRef<'a, Timestamp>,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.compact_batch(&[(collection, new_since.to_owned())])
            .await
    }

    async fn compact_batch<K, V>(
        &mut self,
        compactions: &[(StashCollection<K, V>, Antichain<Timestamp>)],
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        let compactions = compactions.to_owned();
        self.transact(|stmts, tx| {
            let compactions = compactions.clone();
            Box::pin(async move {
                Self::compact_batch_tx(
                    stmts,
                    tx,
                    compactions
                        .iter()
                        .map(|(collection, since)| (collection.id, since)),
                )
                .await
            })
        })
        .await
    }

    async fn consolidate<'a, K, V>(
        &'a mut self,
        collection: StashCollection<K, V>,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.consolidate_batch(&[collection]).await
    }

    async fn consolidate_batch<K, V>(
        &mut self,
        collections: &[StashCollection<K, V>],
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        let collections = collections
            .iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>();
        self.transact(|stmts, tx| {
            let collections = collections.clone();
            Box::pin(async { Self::consolidate_batch_tx(stmts, tx, collections.into_iter()).await })
        })
        .await
    }

    /// Reports the current since frontier.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn since<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError>
    where
        K: Data,
        V: Data,
    {
        self.transact(|stmts, tx| Box::pin(Self::since_tx(stmts, tx, collection.id)))
            .await
    }

    /// Reports the current upper frontier.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn upper<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError>
    where
        K: Data,
        V: Data,
    {
        self.transact(|stmts, tx| Box::pin(Self::upper_tx(stmts, tx, collection.id)))
            .await
    }

    async fn confirm_leadership(&mut self) -> Result<(), StashError> {
        self.transact(|_, _| Box::pin(async { Ok(()) })).await
    }
}

impl From<tokio_postgres::Error> for StashError {
    fn from(e: tokio_postgres::Error) -> StashError {
        StashError {
            inner: InternalStashError::Postgres(e),
        }
    }
}

#[async_trait]
impl Append for Postgres {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn append<I>(&mut self, batches: I) -> Result<(), StashError>
    where
        I: IntoIterator<Item = AppendBatch> + Send + 'static,
        I::IntoIter: Send,
    {
        let batches = batches.into_iter().collect::<Vec<_>>();
        self.transact(move |stmts, tx| {
            let batches = batches.clone();
            Box::pin(async move {
                let mut consolidate = HashSet::new();
                let mut futures = Vec::with_capacity(batches.len());
                for batch in batches {
                    let new_insert = consolidate.insert(batch.collection_id);
                    // Because we pipeline requests, ensure there's only one collection id per
                    // append.
                    assert!(new_insert);
                    futures.push(async move {
                        let upper =
                            Self::update_many_tx(stmts, tx, batch.collection_id, &batch.entries)
                                .await?;
                        if upper != batch.lower {
                            return Err(StashError::from("unexpected lower"));
                        }
                        Self::seal_batch_tx(
                            stmts,
                            tx,
                            std::iter::once((batch.collection_id, &batch.upper)),
                        )
                        .await?;
                        Self::compact_batch_tx(
                            stmts,
                            tx,
                            std::iter::once((batch.collection_id, &batch.compact)),
                        )
                        .await?;
                        Ok(())
                    });
                }
                try_join_all(futures).await?;
                Self::consolidate_batch_tx(stmts, tx, consolidate.into_iter()).await?;
                Ok(())
            })
        })
        .await
    }
}
