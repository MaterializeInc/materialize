// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::marker::PhantomData;
use std::num::NonZeroI64;
use std::sync::{Arc, Mutex};
use std::{cmp, time::Duration};

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use futures::future::{self, try_join3, try_join4, try_join_all, BoxFuture};
use futures::future::{try_join, TryFutureExt};
use futures::StreamExt;
use postgres_openssl::MakeTlsConnector;
use prometheus::{IntCounter, IntCounterVec};
use serde_json::Value;
use timely::progress::frontier::AntichainRef;
use timely::progress::Antichain;
use timely::PartialOrder;
use tokio::sync::mpsc;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Statement};
use tracing::{error, event, info, warn, Level};

use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::retry::Retry;

use crate::{
    consolidate_updates_kv, AntichainFormatter, Append, AppendBatch, Data, Diff, Id,
    InternalStashError, Stash, StashCollection, StashError, Timestamp,
};

// TODO: Change the indexes on data to be more applicable to the current
// consolidation technique. This will involve a migration (which we don't yet
// have code to handle).
const SCHEMA: &str = "
CREATE TABLE fence (
    epoch bigint PRIMARY KEY,
    nonce bytea
);
-- Epochs are guaranteed to be non-zero, so start counting at 1
INSERT INTO fence VALUES (1, '');

-- bigserial is not ideal for Cockroach, but we have a stable number of
-- collections, so our use of it here is fine and compatible with Postgres.
CREATE TABLE collections (
    collection_id bigserial PRIMARY KEY,
    name text NOT NULL UNIQUE
);

CREATE TABLE data (
    collection_id bigint NOT NULL REFERENCES collections (collection_id),
    key jsonb NOT NULL,
    value jsonb NOT NULL,
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
    update_many: Statement,
}

impl PreparedStatements {
    async fn from(client: &Client) -> Result<Self, StashError> {
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
        let update_many = client
            .prepare(
                "INSERT INTO data (collection_id, key, value, time, diff)
             VALUES ($1, $2, $3, $4, $5)",
            )
            .await?;
        Ok(PreparedStatements {
            select_epoch,
            iter_key,
            since,
            upper,
            collection,
            iter,
            seal,
            compact,
            update_many,
        })
    }
}

// Track statement execution counts.
struct CountedStatements<'a> {
    stmts: &'a PreparedStatements,
    // Due to our use of try_join and futures, this needs to be an Arc Mutex.
    // Use a BTreeMap for deterministic debug printing. Use an Option to avoid
    // allocating an Arc when unused.
    counts: Option<Arc<Mutex<BTreeMap<&'static str, usize>>>>,
}

impl<'a> CountedStatements<'a> {
    fn from(stmts: &'a PreparedStatements) -> Self {
        Self {
            stmts,
            counts: if tracing::enabled!(Level::DEBUG) {
                Some(Arc::new(Mutex::new(BTreeMap::new())))
            } else {
                None
            },
        }
    }

    fn inc(&self, name: &'static str) {
        if let Some(counts) = &self.counts {
            let mut map = counts.lock().unwrap();
            *map.entry(name).or_default() += 1;
            *map.entry("_total").or_default() += 1;
        }
    }

    fn select_epoch(&self) -> &Statement {
        self.inc("select_epoch");
        &self.stmts.select_epoch
    }
    fn iter_key(&self) -> &Statement {
        self.inc("iter_key");
        &self.stmts.iter_key
    }
    fn since(&self) -> &Statement {
        self.inc("since");
        &self.stmts.since
    }
    fn upper(&self) -> &Statement {
        self.inc("upper");
        &self.stmts.upper
    }
    fn collection(&self) -> &Statement {
        self.inc("collection");
        &self.stmts.collection
    }
    fn iter(&self) -> &Statement {
        self.inc("iter");
        &self.stmts.iter
    }
    fn seal(&self) -> &Statement {
        self.inc("seal");
        &self.stmts.seal
    }
    fn compact(&self) -> &Statement {
        self.inc("compact");
        &self.stmts.compact
    }
    fn update_many(&self) -> &Statement {
        self.inc("update_many");
        &self.stmts.update_many
    }
}

#[derive(Debug)]
enum TransactionMode {
    /// Transact operations occurs in a normal transaction.
    Writeable,
    /// Transact operations occur in a read-only transaction.
    Readonly,
    /// Transact operations occur in a nested transaction using SAVEPOINTs.
    Savepoint,
}

#[derive(Debug, Clone)]
pub struct PostgresFactory {
    metrics: Arc<Metrics>,
}

impl PostgresFactory {
    pub fn new(registry: &MetricsRegistry) -> PostgresFactory {
        PostgresFactory {
            metrics: Arc::new(Metrics::register_into(registry)),
        }
    }

    /// Opens the stash stored at the specified path.
    pub async fn open(
        &self,
        url: String,
        schema: Option<String>,
        tls: MakeTlsConnector,
    ) -> Result<Postgres, StashError> {
        self.open_inner(TransactionMode::Writeable, url, schema, tls)
            .await
    }

    /// Opens the stash stored at the specified path in readonly mode: any
    /// mutating query will fail, and the epoch is not incremented on start.
    pub async fn open_readonly(
        &self,
        url: String,
        schema: Option<String>,
        tls: MakeTlsConnector,
    ) -> Result<Postgres, StashError> {
        self.open_inner(TransactionMode::Readonly, url, schema, tls)
            .await
    }

    /// Opens the stash stored at the specified path in savepoint mode: mutating
    /// queries are allowed, but they will never be committed, and the epoch is
    /// not incremented on start. This mode is used to test migrations on a
    /// running stash.
    pub async fn open_savepoint(
        &self,
        url: String,
        tls: MakeTlsConnector,
    ) -> Result<Postgres, StashError> {
        self.open_inner(TransactionMode::Savepoint, url, None, tls)
            .await
    }

    async fn open_inner(
        &self,
        txn_mode: TransactionMode,
        url: String,
        schema: Option<String>,
        tls: MakeTlsConnector,
    ) -> Result<Postgres, StashError> {
        let (sinces_tx, mut sinces_rx) = mpsc::unbounded_channel();

        let mut conn = Postgres {
            txn_mode,
            url: url.clone(),
            schema,
            tls: tls.clone(),
            client: None,
            is_cockroach: None,
            statements: None,
            epoch: None,
            // The call to rand::random here assumes that the seed source is from a secure
            // source that will differ per thread. The docs for ThreadRng say it "is
            // automatically seeded from OsRng", which meets this requirement.
            nonce: rand::random(),
            sinces_tx,
            metrics: Arc::clone(&self.metrics),
        };
        // Do the initial connection once here so we don't get stuck in
        // transact's retry loop if the url is bad.
        loop {
            let res = conn.connect().await;
            if let Err(StashError {
                inner: InternalStashError::Postgres(err),
            }) = &res
            {
                // We want this function (`new`) to quickly return an error if
                // the connection string is bad or the server is unreachable. If
                // the server returns a retryable transaction error though,
                // allow it to retry. This is mostly useful for tests which hit
                // this particular error a lot, but is also good for production.
                // See: https://www.cockroachlabs.com/docs/stable/transaction-retry-error-reference.html
                if let Some(dberr) = err.as_db_error() {
                    if dberr.code() == &SqlState::T_R_SERIALIZATION_FAILURE
                        && dberr.message().contains("restart transaction")
                    {
                        warn!("tokio-postgres stash connection error, retrying: {err}");
                        continue;
                    }
                }
            }
            res?;
            break;
        }

        if matches!(conn.txn_mode, TransactionMode::Savepoint) {
            // In savepoint mode, pretend that we're consolidating things.
            mz_ore::task::spawn(|| "stash consolidation dropper", async move {
                while let Some(_) = sinces_rx.recv().await {}
            });
        } else {
            Consolidator::start(url, tls, sinces_rx);
        }

        Ok(conn)
    }
}

#[derive(Debug, Clone)]
struct Metrics {
    transactions: IntCounter,
    transaction_errors: IntCounterVec,
}

impl Metrics {
    pub fn register_into(registry: &MetricsRegistry) -> Metrics {
        Metrics {
            transactions: registry.register(metric!(
                name: "mz_stash_transactions",
                help: "Total number of started transactions.",
            )),
            transaction_errors: registry.register(metric!(
                name: "mz_stash_transaction_errors",
                help: "Total number of transaction errors.",
                var_labels: ["cause"],
            )),
        }
    }
}

/// A Stash whose data is stored in a Postgres database. The format of the
/// tables are not specified and should not be relied upon. The only promise is
/// stability. Any changes to the table schemas will be accompanied by a clear
/// migration path.
pub struct Postgres {
    txn_mode: TransactionMode,
    url: String,
    schema: Option<String>,
    tls: MakeTlsConnector,
    client: Option<Client>,
    is_cockroach: Option<bool>,
    statements: Option<PreparedStatements>,
    epoch: Option<NonZeroI64>,
    nonce: [u8; 16],
    sinces_tx: mpsc::UnboundedSender<(Id, Antichain<Timestamp>)>,
    metrics: Arc<Metrics>,
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
    /// Drops all tables associated with the stash if they exist.
    pub async fn clear(url: &str, tls: MakeTlsConnector) -> Result<(), StashError> {
        let (client, connection) = tokio_postgres::connect(url, tls).await?;
        mz_ore::task::spawn(|| "tokio-postgres stash connection", async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres stash connection error: {}", e);
            }
        });
        client
            .batch_execute(
                "
                BEGIN;
                DROP TABLE IF EXISTS uppers;
                DROP TABLE IF EXISTS sinces;
                DROP TABLE IF EXISTS data;
                DROP TABLE IF EXISTS collections;
                DROP TABLE IF EXISTS fence;
                COMMIT;
            ",
            )
            .await?;
        Ok(())
    }

    /// Verifies stash invariants. Should only be called by tests.
    pub async fn verify(&self) -> Result<(), StashError> {
        let client = self.client.as_ref().unwrap();

        // Because consolidation is in a separate task, allow this to retry.
        Retry::default()
            .max_duration(Duration::from_secs(10))
            .retry_async(|_| async {
                let count: i64 = client
                    .query_one("SELECT count(*) FROM data WHERE diff < 0", &[])
                    .await?
                    .get(0);
                if count > 0 {
                    Err(format!("found {count} data rows with negative diff").into())
                } else {
                    Ok(())
                }
            })
            .await
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
            let tx = client
                .build_transaction()
                .read_only(matches!(self.txn_mode, TransactionMode::Readonly))
                .start()
                .await?;
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
                if !matches!(self.txn_mode, TransactionMode::Writeable) {
                    return Err(format!(
                        "stash tables do not exist; will not create in {:?} mode",
                        self.txn_mode
                    )
                    .into());
                }
                tx.batch_execute(SCHEMA).await?;
            }
            // Bump the epoch, which will cause any previous connection to fail. Add a
            // unique nonce so that if some other thing recreates the entire schema, we
            // can't accidentally have the same epoch, nonce pair (especially risky if the
            // current epoch has been bumped exactly once, then gets recreated by another
            // connection that also bumps it once).
            let epoch = if matches!(self.txn_mode, TransactionMode::Writeable) {
                let row = tx
                    .query_one(
                        "UPDATE fence SET epoch=epoch+1, nonce=$1 RETURNING epoch",
                        &[&self.nonce.to_vec()],
                    )
                    .await?;
                NonZeroI64::new(row.get(0)).unwrap()
            } else {
                let row = tx.query_one("SELECT epoch, nonce FROM fence", &[]).await?;
                let nonce: &[u8] = row.get(1);
                self.nonce = nonce.try_into().map_err(|_| "could not read nonce")?;
                NonZeroI64::new(row.get(0)).unwrap()
            };

            let version: String = tx.query_one("SELECT version()", &[]).await?.get(0);
            let is_cockroach = version.starts_with("CockroachDB");
            if is_cockroach {
                // The `data`, `sinces`, and `uppers` tables can create and delete
                // rows at a high frequency, generating many tombstoned rows. If
                // Cockroach's GC interval is set high (the default is 25h) and
                // these tombstones accumulate, scanning over the table will take
                // increasingly and prohibitively long.
                //
                // See: https://github.com/MaterializeInc/materialize/issues/15842
                // See: https://www.cockroachlabs.com/docs/stable/configure-zone.html#variables
                tx.batch_execute("ALTER TABLE data CONFIGURE ZONE USING gc.ttlseconds = 600;")
                    .await?;
                tx.batch_execute("ALTER TABLE sinces CONFIGURE ZONE USING gc.ttlseconds = 600;")
                    .await?;
                tx.batch_execute("ALTER TABLE uppers CONFIGURE ZONE USING gc.ttlseconds = 600;")
                    .await?;
            }

            tx.commit().await?;
            self.epoch = Some(epoch);
            self.is_cockroach = Some(is_cockroach);
        }

        self.statements = Some(PreparedStatements::from(&client).await?);

        // In savepoint mode start a transaction that will never be committed.
        // Use a low priority so the rw stash won't ever block waiting for the
        // savepoint stash to complete its transaction.
        if matches!(self.txn_mode, TransactionMode::Savepoint) {
            client
                .batch_execute(if self.is_cockroach.unwrap_or(false) {
                    "BEGIN PRIORITY LOW"
                } else {
                    // PRIORITY is a Cockroach extension, so disable it if we
                    // are connecting to Postgres. Needed because some testdrive
                    // tests still use Postgres as the stash backend.
                    "BEGIN"
                })
                .await?;
        }

        self.client = Some(client);
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
            &'a CountedStatements<'a>,
            &'a Client,
        ) -> BoxFuture<'a, Result<T, StashError>>,
    {
        self.metrics.transactions.inc();
        let retry = Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .into_retry_stream();
        let mut retry = Box::pin(retry);
        let mut attempt: u64 = 0;
        loop {
            // Execute the operation in a transaction or savepoint.
            match self.transact_inner(&f).await {
                Ok(r) => return Ok(r),
                Err(e) => {
                    // If this returns an error, close the connection to force a
                    // reconnect (and also not need to worry about any
                    // in-progress transaction state cleanup).
                    self.client = None;
                    match &e.inner {
                        InternalStashError::Postgres(pgerr)
                            if !matches!(self.txn_mode, TransactionMode::Savepoint) =>
                        {
                            // Some errors aren't retryable.
                            if let Some(dberr) = pgerr.as_db_error() {
                                if matches!(
                                    dberr.code(),
                                    &SqlState::UNDEFINED_TABLE
                                        | &SqlState::WRONG_OBJECT_TYPE
                                        | &SqlState::READ_ONLY_SQL_TRANSACTION
                                ) {
                                    return Err(e);
                                }
                            }
                            attempt += 1;
                            let cause = if let Some(code) = pgerr.code() {
                                code.code()
                            } else if pgerr.is_closed() {
                                "closed"
                            } else {
                                "other"
                            };
                            self.metrics
                                .transaction_errors
                                .with_label_values(&[cause])
                                .inc();
                            info!("tokio-postgres stash error, retry attempt {attempt}: {pgerr}");
                            retry.next().await;
                        }
                        _ => return Err(e),
                    }
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn transact_inner<F, T>(&mut self, f: &F) -> Result<T, StashError>
    where
        F: for<'a> Fn(
            &'a CountedStatements<'a>,
            &'a Client,
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
        let stmts = CountedStatements::from(stmts);
        // Generate statements to execute depending on our mode.
        let (tx_start, tx_end) = match self.txn_mode {
            TransactionMode::Writeable => ("BEGIN", "COMMIT"),
            TransactionMode::Readonly => ("BEGIN READ  ONLY", "COMMIT"),
            TransactionMode::Savepoint => ("SAVEPOINT stash", "RELEASE SAVEPOINT stash"),
        };
        client.batch_execute(tx_start).await?;
        // Pipeline the epoch query and closure.
        let epoch_fut = client
            .query_one(stmts.select_epoch(), &[])
            .map_err(|err| err.into());
        let f_fut = f(&stmts, client);
        let (row, res) = future::try_join(epoch_fut, f_fut).await?;
        let current_epoch = NonZeroI64::new(row.get(0)).unwrap();
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
        if let Some(counts) = stmts.counts {
            event!(
                Level::DEBUG,
                counts = format!("{:?}", counts.lock().unwrap()),
            );
        }
        client.batch_execute(tx_end).await?;
        Ok(res)
    }

    async fn since_tx(
        stmts: &CountedStatements<'_>,
        tx: &Client,
        collection_id: Id,
    ) -> Result<Antichain<Timestamp>, StashError> {
        let since: Option<Timestamp> = tx
            .query_one(stmts.since(), &[&collection_id])
            .await?
            .get("since");
        Ok(Antichain::from_iter(since))
    }

    async fn upper_tx(
        stmts: &CountedStatements<'_>,
        tx: &Client,
        collection_id: Id,
    ) -> Result<Antichain<Timestamp>, StashError> {
        let upper: Option<Timestamp> = tx
            .query_one(stmts.upper(), &[&collection_id])
            .await?
            .get("upper");
        Ok(Antichain::from_iter(upper))
    }

    /// `seals` has tuples of `(collection id, new upper, Option<current upper>)`. The
    /// current upper can be `Some` if it is already known.
    async fn seal_batch_tx<'a, I>(
        stmts: &CountedStatements<'_>,
        tx: &Client,
        seals: I,
    ) -> Result<(), StashError>
    where
        I: Iterator<Item = (Id, &'a Antichain<Timestamp>, Option<Antichain<Timestamp>>)>,
    {
        let futures = seals.map(|(collection_id, new_upper, upper)| {
            try_join(
                async move {
                    let upper = match upper {
                        Some(upper) => upper,
                        None => Self::upper_tx(stmts, tx, collection_id).await?,
                    };
                    if PartialOrder::less_than(new_upper, &upper) {
                        return Err(StashError::from(format!(
                            "seal request {} is less than the current upper frontier {}",
                            AntichainFormatter(new_upper),
                            AntichainFormatter(&upper),
                        )));
                    }
                    Ok(())
                },
                async move {
                    tx.execute(stmts.seal(), &[&new_upper.as_option(), &collection_id])
                        .map_err(StashError::from)
                        .await
                },
            )
        });
        try_join_all(futures).await?;
        Ok(())
    }

    /// `upper` can be `Some` if the collection's upper is already known.
    async fn update_many_tx(
        stmts: &CountedStatements<'_>,
        tx: &Client,
        collection_id: Id,
        entries: &[((Value, Value), Timestamp, Diff)],
        upper: Option<Antichain<Timestamp>>,
    ) -> Result<(), StashError> {
        let mut futures = Vec::with_capacity(entries.len());
        for ((key, value), time, diff) in entries {
            futures.push(async move {
                tx.execute(
                    stmts.update_many(),
                    &[&collection_id, &key, &value, &time, &diff],
                )
                .map_err(|err| err.into())
                .await
            });
        }
        // Check the upper in a separate future so we can issue the updates without
        // waiting for it first.
        try_join(
            async {
                let upper = match upper {
                    Some(upper) => upper,
                    None => Self::upper_tx(stmts, tx, collection_id).await?,
                };
                for ((_key, _value), time, _diff) in entries {
                    if !upper.less_equal(time) {
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
        Ok(())
    }

    /// `compactions` has tuples of `(collection id, new since, Option<current
    /// upper>)`. The current upper can be `Some` if it is already known.
    async fn compact_batch_tx<'a, I>(
        stmts: &CountedStatements<'_>,
        tx: &Client,
        compactions: I,
    ) -> Result<(), StashError>
    where
        I: Iterator<Item = (Id, &'a Antichain<Timestamp>, Option<Antichain<Timestamp>>)>,
    {
        let futures = compactions.map(|(collection_id, new_since, upper)| {
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
                    let upper = match upper {
                        Some(upper) => upper,
                        None => Self::upper_tx(stmts, tx, collection_id).await?,
                    };
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
                    tx.execute(stmts.compact(), &[&new_since.as_option(), &collection_id])
                        .map_err(StashError::from)
                        .await
                },
            )
        });
        try_join_all(futures).await?;
        Ok(())
    }

    /// Returns sinces for the requested collections.
    async fn sinces_batch_tx(
        stmts: &CountedStatements<'_>,
        tx: &Client,
        collections: &[Id],
    ) -> Result<HashMap<Id, Antichain<Timestamp>>, StashError> {
        let mut futures = Vec::with_capacity(collections.len());
        for collection_id in collections {
            futures.push(async move {
                let since = Self::since_tx(stmts, tx, *collection_id).await?;
                // Without this type assertion, we get a "type inside `async fn` body must be
                // known in this context" error.
                Result::<_, StashError>::Ok((*collection_id, since))
            });
        }
        let sinces = HashMap::from_iter(try_join_all(futures).await?);
        Ok(sinces)
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
                    .query_one(stmts.collection(), &[&name])
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

    async fn collections(&mut self) -> Result<BTreeSet<String>, StashError> {
        let names = self
            .transact(move |_stmts, tx| {
                Box::pin(async move {
                    let rows = tx.query("SELECT name FROM collections", &[]).await?;
                    Ok(rows.into_iter().map(|row| row.get(0)))
                })
            })
            .await?;
        Ok(BTreeSet::from_iter(names))
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
                let rows = tx
                    .query(stmts.iter(), &[&collection.id])
                    .await?
                    .into_iter()
                    .map(|row| {
                        let key: Value = row.try_get("key")?;
                        let value: Value = row.try_get("value")?;
                        let time = row.try_get("time")?;
                        let diff: Diff = row.try_get("diff")?;
                        Ok::<_, StashError>(((key, value), cmp::max(time, since), diff))
                    })
                    // The collect here isn't needed, we just want the short circuit return
                    // behavior of ?. Is there a way to achieve that without allocating a Vec?
                    .collect::<Result<Vec<_>, _>>()?;
                let rows = consolidate_updates_kv(rows).collect();
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
        let key = serde_json::to_vec(key).expect("must serialize");
        let key: Value = serde_json::from_slice(&key)?;
        self.transact(move |stmts, tx| {
            let key = key.clone();
            Box::pin(async move {
                let (since, rows) = future::try_join(
                    Self::since_tx(stmts, tx, collection.id),
                    tx.query(stmts.iter_key(), &[&collection.id, &key])
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
                        let value: Value = row.try_get("value")?;
                        let value: V = serde_json::from_value(value)?;
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
                let key = serde_json::to_value(&key).expect("must serialize");
                let value = serde_json::to_value(&value).expect("must serialize");
                ((key, value), time, diff)
            })
            .collect::<Vec<_>>();
        self.transact(move |stmts, tx| {
            let entries = entries.clone();
            Box::pin(async move {
                Self::update_many_tx(stmts, tx, collection.id, &entries, None).await?;
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
                Self::seal_batch_tx(stmts, tx, seals.iter().map(|d| (d.0, &d.1, None))).await
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
        let compactions = compactions
            .iter()
            .map(|(collection, since)| (collection.id, since.clone()))
            .collect::<Vec<_>>();
        self.transact(|stmts, tx| {
            let compactions = compactions.clone();
            Box::pin(async move {
                Self::compact_batch_tx(
                    stmts,
                    tx,
                    compactions.iter().map(|(id, since)| (*id, since, None)),
                )
                .await
            })
        })
        .await
    }

    async fn consolidate(&mut self, collection: Id) -> Result<(), StashError> {
        self.consolidate_batch(&[collection]).await
    }

    async fn consolidate_batch(&mut self, collections: &[Id]) -> Result<(), StashError> {
        let collections = collections.to_vec();
        let sinces = self
            .transact(|stmts, tx| {
                let collections = collections.clone();
                Box::pin(async move { Self::sinces_batch_tx(stmts, tx, &collections).await })
            })
            .await?;
        // On successful transact, send consolidation sinces to the
        // Consolidator.
        for (id, since) in sinces {
            self.sinces_tx
                .send((id, since))
                .expect("consolidator unexpectedly gone");
        }
        Ok(())
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

    fn epoch(&self) -> Option<NonZeroI64> {
        self.epoch
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
    async fn append_batch(&mut self, batches: &[AppendBatch]) -> Result<(), StashError> {
        if batches.is_empty() {
            return Ok(());
        }
        let batches = batches.to_vec();
        self.transact(move |stmts, tx| {
            let batches = batches.clone();
            Box::pin(async move {
                let futures = batches.into_iter().map(
                    |AppendBatch {
                         collection_id,
                         lower,
                         upper,
                         compact,
                         entries,
                         ..
                     }| {
                        // Clone to appease rust async.
                        let lower1 = lower.clone();
                        let lower2 = lower.clone();
                        let upper1 = upper.clone();
                        try_join4(
                            async move {
                                let current_upper =
                                    Self::upper_tx(stmts, tx, collection_id).await?;
                                if current_upper != lower1 {
                                    return Err(StashError::from("unexpected lower"));
                                }
                                Ok(())
                            },
                            async move {
                                Self::update_many_tx(
                                    stmts,
                                    tx,
                                    collection_id,
                                    &entries,
                                    Some(lower2),
                                )
                                .await
                            },
                            async move {
                                Self::seal_batch_tx(
                                    stmts,
                                    tx,
                                    std::iter::once((collection_id, &upper1, Some(lower))),
                                )
                                .await
                            },
                            async move {
                                Self::compact_batch_tx(
                                    stmts,
                                    tx,
                                    std::iter::once((collection_id, &compact, Some(upper))),
                                )
                                .await
                            },
                        )
                    },
                );
                try_join_all(futures).await
            })
        })
        .await?;
        Ok(())
    }
}

/// The Consolidator receives since advancements on a channel and
/// transactionally consolidates them. These can safely be done at a later time
/// in a separate connection that doesn't do leader or epoch checking because 1)
/// having data that needs to be consolidated is not a correctness error and 2)
/// the operations here are idempotent (can safely be run concurrently with a
/// second stash).
struct Consolidator {
    url: String,
    tls: MakeTlsConnector,
    sinces_rx: mpsc::UnboundedReceiver<(Id, Antichain<Timestamp>)>,
    consolidations: HashMap<Id, Antichain<Timestamp>>,

    client: Option<Client>,
    stmt_candidates: Option<Statement>,
    stmt_insert: Option<Statement>,
    stmt_delete: Option<Statement>,
}

impl Consolidator {
    pub fn start(
        url: String,
        tls: MakeTlsConnector,
        sinces_rx: mpsc::UnboundedReceiver<(Id, Antichain<Timestamp>)>,
    ) {
        let cons = Self {
            url,
            tls,
            sinces_rx,
            client: None,
            stmt_candidates: None,
            stmt_insert: None,
            stmt_delete: None,
            consolidations: HashMap::new(),
        };
        cons.spawn();
    }

    fn spawn(mut self) {
        // Do consolidation automatically, in a separate connection, and only
        // for things that might benefit from it (have had a negative diff
        // applied).
        mz_ore::task::spawn(|| "stash consolidation", async move {
            // Wait for the next consolidation request.
            while let Some((id, ts)) = self.sinces_rx.recv().await {
                self.insert(id, ts);

                while !self.consolidations.is_empty() {
                    // Accumulate any pending requests that have come in during
                    // our work so we can attempt to get the most recent since
                    // for a quickly advancing collection.
                    while let Ok((id, ts)) = self.sinces_rx.try_recv() {
                        self.insert(id, ts);
                    }

                    // Pick a random key to consolidate.
                    let id = *self.consolidations.keys().next().expect("must exist");
                    let ts = self.consolidations.remove(&id).expect("must exist");

                    // Duplicate the loop-retry-connect structure as in the
                    // transact function by forcing reconnects anytime an error
                    // occurs.
                    let retry = Retry::default()
                        .clamp_backoff(Duration::from_secs(1))
                        .into_retry_stream();
                    let mut retry = Box::pin(retry);
                    let mut attempt: u64 = 0;
                    loop {
                        match self.consolidate(id, &ts).await {
                            Ok(()) => break,
                            Err(e) => {
                                attempt += 1;
                                error!("tokio-postgres stash consolidation error, retry attempt {attempt}: {e}");
                                self.client = None;
                                retry.next().await;
                            }
                        }
                    }
                }
            }
        });
    }

    // Update the set of pending consolidations to the most recent since
    // we've received for a collection.
    fn insert(&mut self, id: Id, ts: Antichain<Timestamp>) {
        self.consolidations
            .entry(id)
            .and_modify(|e| e.join_assign(&ts))
            .or_insert(ts);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn consolidate(
        &mut self,
        id: Id,
        since: &Antichain<Timestamp>,
    ) -> Result<(), StashError> {
        if self.client.is_none() {
            self.connect().await?;
        }
        let client = self.client.as_mut().unwrap();
        let tx = client.transaction().await?;
        let deleted = match since.borrow().as_option() {
            Some(since) => {
                // In a single query we can detect all candidate entries (things
                // with a negative diff) and delete and return all associated
                // keys.
                let rows = tx
                    .query(self.stmt_candidates.as_ref().unwrap(), &[&id, since])
                    .await?
                    .into_iter()
                    .map(|row| {
                        (
                            (
                                row.get::<_, serde_json::Value>("key"),
                                row.get::<_, serde_json::Value>("value"),
                            ),
                            row.get::<_, Diff>("diff"),
                        )
                    })
                    .collect::<Vec<_>>();
                let deleted = rows.len();
                // Perform the consolidation in Rust.
                let rows = crate::consolidate(rows);
                // Then for any items that have a positive diff, INSERT them
                // back into the database. Our current production stash usage
                // will never have any results here (all consolidations sum to
                // 0), only tests will. Thus, it's probably faster to perform
                // consolidations in Rust instead of SQL because (unverified
                // assumption) it's faster to return all the rows and use
                // differential's consolidation method. So far we have not
                // produced a benchmark that can accurately verify these claims.
                // The benchmarks we have thus far are either not this workload
                // or else vary wildly when the exact same benchmark is run
                // repeatedly.
                for ((key, value), diff) in rows {
                    tx.execute(
                        self.stmt_insert.as_ref().unwrap(),
                        &[&id, &key, &value, since, &diff],
                    )
                    .await?;
                }
                mz_ore::cast::usize_to_u64(deleted)
            }
            None => {
                // The since is empty, so we can delete all the associated data.
                tx.execute(self.stmt_delete.as_ref().unwrap(), &[&id])
                    .await?
            }
        };
        tx.commit().await?;
        event!(Level::DEBUG, deleted);
        Ok(())
    }

    async fn connect(&mut self) -> Result<(), StashError> {
        let (client, connection) = tokio_postgres::connect(&self.url, self.tls.clone()).await?;
        mz_ore::task::spawn(
            || "tokio-postgres stash consolidation connection",
            async move {
                if let Err(e) = connection.await {
                    tracing::error!("postgres stash connection error: {}", e);
                }
            },
        );
        self.stmt_candidates = Some(
            client
                .prepare(
                    "
                    DELETE FROM data
                    WHERE collection_id = $1 AND time <= $2 AND key IN (
                        SELECT key
                        FROM data
                        WHERE collection_id = $1 AND time <= $2 AND diff < 0
                    )
                    RETURNING key, value, diff
                    ",
                )
                .await?,
        );
        self.stmt_insert = Some(
            client
                .prepare(
                    "INSERT INTO data (collection_id, key, value, time, diff)
                    VALUES ($1, $2, $3, $4, $5)",
                )
                .await?,
        );
        self.stmt_delete = Some(
            client
                .prepare("DELETE FROM data WHERE collection_id = $1")
                .await?,
        );
        self.client = Some(client);
        Ok(())
    }
}
