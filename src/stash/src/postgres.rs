// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::TryFutureExt;
use futures::future::{self, try_join_all, BoxFuture};
use futures::StreamExt;
use postgres_openssl::MakeTlsConnector;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Statement, Transaction};
use tracing::warn;

use mz_ore::retry::Retry;

use crate::{
    Append, AppendBatch, Data, Diff, Id, InternalStashError, Stash, StashCollection, StashError,
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
    collection_id bigint REFERENCES collections (collection_id),
    key bytea,
    value bytea NOT NULL,
    PRIMARY KEY (collection_id, key)
);
";

struct PreparedStatements {
    select_epoch: Statement,
    get_key: Statement,
    collection: Statement,
    iter: Statement,
    update_many: Statement,
    delete: Statement,
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
        let get_key = client
            .prepare(
                "SELECT value FROM data
                 WHERE collection_id = $1 AND key = $2",
            )
            .await?;
        let collection = client
            .prepare("SELECT collection_id FROM collections WHERE name = $1")
            .await?;
        let iter = client
            .prepare(
                "SELECT key, value FROM data
                 WHERE collection_id = $1",
            )
            .await?;
        let update_many = client
            .prepare(
                "INSERT INTO data (collection_id, key, value)
                 VALUES ($1, $2, $3)
                 ON CONFLICT DO NOTHING",
            )
            .await?;
        let delete = client
            .prepare(
                "DELETE FROM data
                 WHERE collection_id = $1 AND key = $2",
            )
            .await?;
        self.client = Some(client);
        self.statements = Some(PreparedStatements {
            select_epoch,
            get_key,
            collection,
            iter,
            update_many,
            delete,
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

    /// `upper` can be `Some` if the collection's upper is already known.
    async fn update_many_tx(
        stmts: &PreparedStatements,
        tx: &Transaction<'_>,
        collection_id: Id,
        entries: &[((Vec<u8>, Vec<u8>), Diff)],
    ) -> Result<(), StashError> {
        let mut futures = Vec::with_capacity(entries.len());
        for ((key, value), diff) in entries {
            futures.push(async move {
                match diff {
                    Diff::Insert => {
                        let count = tx
                            .execute(&stmts.update_many, &[&collection_id, &key, &value])
                            .map_err(StashError::from)
                            .await?;
                        if count == 0 {
                            return Err(StashError::from(InternalStashError::InsertDuplicateKey));
                        }
                    }
                    Diff::Delete => {
                        let count = tx
                            .execute(&stmts.delete, &[&collection_id, &key])
                            .map_err(StashError::from)
                            .await?;
                        if count == 0 {
                            return Err(StashError::from(InternalStashError::DeleteNotFound));
                        }
                    }
                };
                Ok(())
            });
        }
        try_join_all(futures).await?;
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
    ) -> Result<BTreeMap<K, V>, StashError>
    where
        K: Data,
        V: Data,
    {
        self.transact(move |stmts, tx| {
            Box::pin(async move {
                let rows = tx
                    .query(&stmts.iter, &[&collection.id])
                    .await?
                    .into_iter()
                    .map(|row| {
                        let key_buf: Vec<_> = row.try_get("key")?;
                        let value_buf: Vec<_> = row.try_get("value")?;
                        let key = K::decode(&key_buf)?;
                        let value = V::decode(&value_buf)?;
                        Ok::<_, StashError>((key, value))
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()?;
                Ok(rows)
            })
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_key<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Option<V>, StashError>
    where
        K: Data,
        V: Data,
    {
        let mut key_buf = vec![];
        key.encode(&mut key_buf);
        self.transact(move |stmts, tx| {
            let key_buf = key_buf.clone();
            Box::pin(async move {
                let value = tx
                    .query_opt(&stmts.get_key, &[&collection.id, &key_buf])
                    .map_err(StashError::from)
                    .await?
                    .map(|row| {
                        let value_buf: Vec<_> = row.try_get("value")?;
                        let value = V::decode(&value_buf)?;
                        Ok::<_, StashError>(value)
                    });
                Ok(value.transpose()?)
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
        I: IntoIterator<Item = ((K, V), Diff)> + Send,
        I::IntoIter: Send,
    {
        let entries = entries
            .into_iter()
            .map(|((key, value), diff)| {
                let mut key_buf = vec![];
                let mut value_buf = vec![];
                key.encode(&mut key_buf);
                value.encode(&mut value_buf);
                ((key_buf, value_buf), diff)
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
                let mut futures = Vec::with_capacity(batches.len());
                for batch in batches {
                    futures.push(async move {
                        Self::update_many_tx(stmts, tx, batch.collection_id, &batch.entries).await
                    });
                }
                try_join_all(futures).await?;
                Ok(())
            })
        })
        .await
    }
}
