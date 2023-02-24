// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::num::NonZeroI64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use fail::fail_point;
use futures::future::{self, BoxFuture};
use futures::future::{FutureExt, TryFutureExt};
use futures::{Future, StreamExt};
use postgres_openssl::MakeTlsConnector;
use prometheus::{IntCounter, IntCounterVec};
use rand::Rng;

use timely::progress::Antichain;
use tokio::sync::mpsc;
use tokio::time::Interval;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Statement};
use tracing::{error, event, info, warn, Level};

use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::retry::Retry;

use crate::{
    AppendBatch, Data, Diff, Id, InternalStashError, StashCollection, StashError, Timestamp,
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

// Force reconnection every few minutes to allow cockroach to rebalance
// connections after it restarts during maintenance or upgrades.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(300);

struct PreparedStatements {
    select_epoch: Statement,
    iter_key: Statement,
    since: Statement,
    upper: Statement,
    collection: Statement,
    iter: Statement,
    seal: Statement,
    compact: Statement,
    update_many: Arc<tokio::sync::Mutex<BTreeMap<usize, Statement>>>,
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
        Ok(PreparedStatements {
            select_epoch,
            iter_key,
            since,
            upper,
            collection,
            iter,
            seal,
            compact,
            update_many: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        })
    }
}

// Track statement execution counts.
pub(crate) struct CountedStatements<'a> {
    stmts: &'a PreparedStatements,
    // Due to our use of try_join and futures, this needs to be an Arc Mutex.
    // Use a BTreeMap for deterministic debug printing. Use an Option to avoid
    // allocating an Arc when unused.
    counts: Option<Arc<Mutex<BTreeMap<String, usize>>>>,
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

    pub fn inc<S: Into<String>>(&self, name: S) {
        if let Some(counts) = &self.counts {
            let mut map = counts.lock().unwrap();
            *map.entry(name.into()).or_default() += 1;
            *map.entry("_total".into()).or_default() += 1;
        }
    }

    pub fn select_epoch(&self) -> &Statement {
        self.inc("select_epoch");
        &self.stmts.select_epoch
    }
    pub fn iter_key(&self) -> &Statement {
        self.inc("iter_key");
        &self.stmts.iter_key
    }
    pub fn since(&self) -> &Statement {
        self.inc("since");
        &self.stmts.since
    }
    pub fn upper(&self) -> &Statement {
        self.inc("upper");
        &self.stmts.upper
    }
    pub fn collection(&self) -> &Statement {
        self.inc("collection");
        &self.stmts.collection
    }
    pub fn iter(&self) -> &Statement {
        self.inc("iter");
        &self.stmts.iter
    }
    pub fn seal(&self) -> &Statement {
        self.inc("seal");
        &self.stmts.seal
    }
    pub fn compact(&self) -> &Statement {
        self.inc("compact");
        &self.stmts.compact
    }
    /// Returns a ToStatement to INSERT a specified number of rows. First
    /// statement parameter is collection_id. Then key, value, time, diff as
    /// sets of 4 for each row.
    pub async fn update(&self, client: &Client, rows: usize) -> Result<Statement, StashError> {
        self.inc(format!("update[{rows}]"));

        match self.stmts.update_many.lock().await.entry(rows) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let mut stmt =
                    String::from("INSERT INTO data (collection_id, key, value, time, diff) VALUES");
                let mut sep = ' ';
                for i in 0..rows {
                    let idx = 1 + i * 4;
                    write!(
                        &mut stmt,
                        "{}($1, ${}, ${}, ${}, ${})",
                        sep,
                        idx + 1,
                        idx + 2,
                        idx + 3,
                        idx + 4
                    )
                    .unwrap();
                    sep = ',';
                }
                let stmt = client.prepare(&stmt).await?;
                Ok(entry.insert(stmt).clone())
            }
        }
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
pub struct StashFactory {
    metrics: Arc<Metrics>,
}

impl StashFactory {
    pub fn new(registry: &MetricsRegistry) -> StashFactory {
        StashFactory {
            metrics: Arc::new(Metrics::register_into(registry)),
        }
    }

    /// Opens the stash stored at the specified path.
    pub async fn open(
        &self,
        url: String,
        schema: Option<String>,
        tls: MakeTlsConnector,
    ) -> Result<Stash, StashError> {
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
    ) -> Result<Stash, StashError> {
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
    ) -> Result<Stash, StashError> {
        self.open_inner(TransactionMode::Savepoint, url, None, tls)
            .await
    }

    async fn open_inner(
        &self,
        txn_mode: TransactionMode,
        url: String,
        schema: Option<String>,
        tls: MakeTlsConnector,
    ) -> Result<Stash, StashError> {
        let (sinces_tx, mut sinces_rx) = mpsc::unbounded_channel();

        let mut conn = Stash {
            txn_mode,
            url: url.clone(),
            schema,
            tls: tls.clone(),
            client: None,
            reconnect: tokio::time::interval(RECONNECT_INTERVAL),
            statements: None,
            epoch: None,
            // The call to rand::random here assumes that the seed source is from a secure
            // source that will differ per thread. The docs for ThreadRng say it "is
            // automatically seeded from OsRng", which meets this requirement.
            nonce: rand::random(),
            sinces_tx,
            metrics: Arc::clone(&self.metrics),
            collections: BTreeMap::new(),
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
        let metrics = Metrics {
            transactions: registry.register(metric!(
                name: "mz_stash_transactions",
                help: "Total number of started transactions.",
            )),
            transaction_errors: registry.register(metric!(
                name: "mz_stash_transaction_errors",
                help: "Total number of transaction errors.",
                var_labels: ["cause"],
            )),
        };
        // Initialize error codes to 0 so we can observe their increase.
        metrics
            .transaction_errors
            .with_label_values(&["closed"])
            .inc_by(0);
        metrics
            .transaction_errors
            .with_label_values(&["retry"])
            .inc_by(0);
        metrics
            .transaction_errors
            .with_label_values(&["other"])
            .inc_by(0);
        metrics
    }
}

/// A Stash whose data is stored in a Postgres database. The format of the
/// tables are not specified and should not be relied upon. The only promise is
/// stability. Any changes to the table schemas will be accompanied by a clear
/// migration path.
pub struct Stash {
    txn_mode: TransactionMode,
    url: String,
    schema: Option<String>,
    tls: MakeTlsConnector,
    client: Option<Client>,
    reconnect: Interval,

    statements: Option<PreparedStatements>,
    epoch: Option<NonZeroI64>,
    nonce: [u8; 16],
    pub(crate) sinces_tx: mpsc::UnboundedSender<(Id, Antichain<Timestamp>)>,
    pub(crate) collections: BTreeMap<String, Id>,
    metrics: Arc<Metrics>,
}

impl std::fmt::Debug for Stash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Postgres")
            .field("url", &self.url)
            .field("epoch", &self.epoch)
            .field("nonce", &self.nonce)
            .finish_non_exhaustive()
    }
}

impl Stash {
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

    /// Creates a debug stash from the current COCKROACH_URL with a random
    /// schema, and DROPs it after `f` has returned.
    pub async fn with_debug_stash<F, T, Fut>(f: F) -> Result<T, StashError>
    where
        F: FnOnce(Stash) -> Fut,
        Fut: Future<Output = T>,
    {
        let factory = DebugStashFactory::try_new().await?;
        let stash = factory.try_open_debug().await?;
        Ok(f(stash).await)
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
                    .await
                    .expect("verify select count failed")
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
            let epoch = if matches!(self.txn_mode, TransactionMode::Writeable) {
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

                // Bump the epoch, which will cause any previous connection to fail. Add a
                // unique nonce so that if some other thing recreates the entire schema, we
                // can't accidentally have the same epoch, nonce pair (especially risky if the
                // current epoch has been bumped exactly once, then gets recreated by another
                // connection that also bumps it once).
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

            tx.commit().await?;
            self.epoch = Some(epoch);
        }

        self.statements = Some(PreparedStatements::from(&client).await?);

        // In savepoint mode start a transaction that will never be committed.
        // Use a low priority so the rw stash won't ever block waiting for the
        // savepoint stash to complete its transaction.
        if matches!(self.txn_mode, TransactionMode::Savepoint) {
            client.batch_execute("BEGIN PRIORITY LOW").await?;
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
    #[tracing::instrument(name = "stash::transact", level = "debug", skip_all)]
    pub(crate) async fn transact<F, T>(&mut self, f: F) -> Result<T, StashError>
    where
        F: for<'a> Fn(
            &'a CountedStatements<'a>,
            &'a Client,
            &'a BTreeMap<String, Id>,
        ) -> BoxFuture<'a, Result<T, StashError>>,
    {
        self.metrics.transactions.inc();
        let retry = Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .into_retry_stream();
        let mut retry = Box::pin(retry);
        let mut attempt: u64 = 0;

        // Actively reconnect to allow cockroach to rebalanace.
        if self.reconnect.tick().now_or_never().is_some() {
            self.client = None;
        }

        loop {
            // Execute the operation in a transaction or savepoint.
            match self.transact_inner(&f).await {
                Ok(r) => return Ok(r),
                Err(e) => {
                    // If this returns an error, close the connection to force a
                    // reconnect (and also not need to worry about any
                    // in-progress transaction state cleanup).
                    self.client = None;

                    attempt += 1;
                    let cause = e.cause();
                    self.metrics
                        .transaction_errors
                        .with_label_values(&[cause])
                        .inc();
                    info!(
                        "tokio-postgres stash error, retry attempt {attempt}: {}, code: {:?}",
                        e.inner(),
                        e.code(),
                    );

                    // Savepoint is never retryable because we can't restore all previous
                    // savepoints.
                    if matches!(self.txn_mode, TransactionMode::Savepoint) {
                        return Err(e.into());
                    }

                    if e.retryable() {
                        // Retry only known safe errors. Others need to cause a
                        // fatal crash in environmentd because a transaction
                        // could have committed without us receiving the commit
                        // confirmation
                        retry.next().await;
                    } else {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    #[tracing::instrument(name = "stash::transact_inner", level = "debug", skip_all)]
    async fn transact_inner<F, T>(&mut self, f: &F) -> Result<T, TransactionError>
    where
        F: for<'a> Fn(
            &'a CountedStatements<'a>,
            &'a Client,
            &'a BTreeMap<String, Id>,
        ) -> BoxFuture<'a, Result<T, StashError>>,
    {
        let reconnect = match &self.client {
            Some(client) => client.is_closed(),
            None => true,
        };
        if reconnect {
            self.connect().await.map_err(TransactionError::Connect)?;
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
        client
            .batch_execute(tx_start)
            .await
            .map_err(|err| TransactionError::Txn(err.into()))?;
        // Pipeline the epoch query and closure.
        let epoch_fut = client
            .query_one(stmts.select_epoch(), &[])
            .map_err(|err| err.into());
        let f_fut = f(&stmts, client, &self.collections);
        let (row, res) = future::try_join(epoch_fut, f_fut)
            .await
            .map_err(TransactionError::Txn)?;
        let current_epoch = NonZeroI64::new(row.get(0)).unwrap();
        if Some(current_epoch) != self.epoch {
            return Err(TransactionError::Epoch(
                InternalStashError::Fence(format!(
                    "unexpected fence epoch {}, expected {:?}",
                    current_epoch, self.epoch
                ))
                .into(),
            ));
        }
        let current_nonce: Vec<u8> = row.get(1);
        if current_nonce != self.nonce {
            return Err(TransactionError::Epoch(
                InternalStashError::Fence("unexpected fence nonce".into()).into(),
            ));
        }
        if let Some(counts) = stmts.counts {
            event!(
                Level::DEBUG,
                counts = format!("{:?}", counts.lock().unwrap()),
            );
        }
        client
            .batch_execute(tx_end)
            .await
            .map_err(|err| TransactionError::Commit(err.into()))?;

        fail_point!("stash_commit", |r| Err(TransactionError::Commit(
            r.unwrap_or_else(|| "stash_commit failpoint".to_string())
                .into()
        )));

        Ok(res)
    }
}

#[derive(Debug)]
pub(crate) enum TransactionError {
    /// A failure occurred pre-transaction.
    Connect(StashError),
    /// The epoch check failed.
    Epoch(StashError),
    /// The transaction function failed and the commit was never started.
    Txn(StashError),
    /// The commit was started and failed.
    Commit(StashError),
}

impl From<TransactionError> for StashError {
    fn from(err: TransactionError) -> StashError {
        InternalStashError::Transaction(Box::new(err)).into()
    }
}

impl TransactionError {
    fn inner(&self) -> &StashError {
        match self {
            TransactionError::Connect(err)
            | TransactionError::Epoch(err)
            | TransactionError::Txn(err)
            | TransactionError::Commit(err) => err,
        }
    }

    fn pgerr(&self) -> Option<&tokio_postgres::Error> {
        if let InternalStashError::Postgres(err) = &self.inner().inner {
            Some(err)
        } else {
            None
        }
    }

    fn code(&self) -> Option<&SqlState> {
        self.pgerr().and_then(|err| err.code())
    }

    fn is_closed(&self) -> bool {
        match self.pgerr() {
            Some(err) => err.is_closed(),
            None => false,
        }
    }

    fn cause(&self) -> &str {
        if self.is_closed() {
            "closed"
        } else if let Some(&SqlState::T_R_SERIALIZATION_FAILURE) = self.code() {
            "retry"
        } else {
            "other"
        }
    }

    /// Reports whether this error can safely be retried.
    pub fn retryable(&self) -> bool {
        // Only attempt to retry postgres-related errors. Others come from stash
        // code and can't be retried.
        if !matches!(self.inner().inner, InternalStashError::Postgres(_)) {
            return false;
        }

        // Check some known permanent failure codes.
        if matches!(
            self.code(),
            Some(&SqlState::UNDEFINED_TABLE)
                | Some(&SqlState::WRONG_OBJECT_TYPE)
                | Some(&SqlState::READ_ONLY_SQL_TRANSACTION)
        ) {
            return false;
        }

        match self {
            // Always retry if the initial connection failed.
            TransactionError::Connect(_) => true,
            // Never retry if the epoch check failed.
            TransactionError::Epoch(_) => false,
            // Retry inner transaction failures.
            TransactionError::Txn(_) => true,
            TransactionError::Commit(_) => {
                // If the failure occurred during the commit attempt, only retry
                // if we got an explicit code from the database notifying us
                // that this is possible. A connection error or perhaps any
                // other error could have left the stash in an unknown state.
                // Until we are idempotent or able to recover from this, our
                // only choice is to issue a fatal failure, forcing the caller
                // to restart its process and reinitialize its memory from fully
                // reading the stash.
                matches!(self.code(), Some(&SqlState::T_R_SERIALIZATION_FAILURE))
            }
        }
    }
}

impl Stash {
    pub async fn collection<K, V>(
        &mut self,
        name: &str,
    ) -> Result<StashCollection<K, V>, StashError>
    where
        K: Data,
        V: Data,
    {
        let name = name.to_string();
        self.with_transaction(move |tx| Box::pin(async move { tx.collection(&name).await }))
            .await
    }

    pub async fn collections(&mut self) -> Result<BTreeSet<String>, StashError> {
        self.with_transaction(move |tx| Box::pin(async move { tx.collections().await }))
            .await
    }

    pub async fn consolidate(&mut self, collection: Id) -> Result<(), StashError> {
        self.consolidate_batch(&[collection]).await
    }

    pub async fn consolidate_batch(&mut self, collections: &[Id]) -> Result<(), StashError> {
        let collections = collections.to_vec();
        let sinces = self
            .with_transaction(move |tx| {
                Box::pin(async move { tx.sinces_batch(&collections).await })
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

    pub async fn confirm_leadership(&mut self) -> Result<(), StashError> {
        self.with_transaction(|_| Box::pin(async { Ok(()) })).await
    }

    pub fn is_readonly(&self) -> bool {
        matches!(self.txn_mode, TransactionMode::Readonly)
    }

    pub fn epoch(&self) -> Option<NonZeroI64> {
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

impl Stash {
    #[tracing::instrument(level = "debug", skip_all)]
    /// Like `append` but doesn't consolidate.
    pub async fn append_batch(&mut self, batches: Vec<AppendBatch>) -> Result<(), StashError> {
        if batches.is_empty() {
            return Ok(());
        }
        self.with_transaction(move |tx| {
            Box::pin(async move {
                let batches = batches.clone();
                tx.append(batches).await
            })
        })
        .await
    }

    /// Atomically adds entries, seals, compacts, and consolidates multiple
    /// collections.
    ///
    /// The `lower` of each `AppendBatch` is checked to be the existing `upper` of the collection.
    /// The `upper` of the `AppendBatch` will be the new `upper` of the collection.
    /// The `compact` of each `AppendBatch` will be the new `since` of the collection.
    ///
    /// If this method returns `Ok`, the entries have been made durable and uppers
    /// advanced, otherwise no changes were committed.
    pub async fn append(&mut self, batches: Vec<AppendBatch>) -> Result<(), StashError> {
        if batches.is_empty() {
            return Ok(());
        }
        let ids: Vec<_> = batches.iter().map(|batch| batch.collection_id).collect();
        self.append_batch(batches).await?;
        self.consolidate_batch(&ids).await?;
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
    consolidations: BTreeMap<Id, Antichain<Timestamp>>,

    client: Option<Client>,
    reconnect: Interval,
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
            reconnect: tokio::time::interval(RECONNECT_INTERVAL),
            stmt_candidates: None,
            stmt_insert: None,
            stmt_delete: None,
            consolidations: BTreeMap::new(),
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

                if self.reconnect.tick().now_or_never().is_some() {
                    self.client = None;
                }

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

/// Stash factory to use for tests that uses a random schema for a stash, which is re-used on all
/// stash openings. The schema is dropped when this factory is dropped.
pub struct DebugStashFactory {
    url: String,
    schema: String,
    tls: MakeTlsConnector,
    stash_factory: StashFactory,
}

impl DebugStashFactory {
    /// Returns a new factory that will generate a random schema one time, then use it on any
    /// opened Stash.
    pub async fn try_new() -> Result<DebugStashFactory, StashError> {
        let url =
            std::env::var("COCKROACH_URL").expect("COCKROACH_URL environment variable is not set");
        let rng: usize = rand::thread_rng().gen();
        let schema = format!("schema_{rng}");
        let tls = mz_postgres_util::make_tls(&tokio_postgres::Config::new()).unwrap();

        let (client, connection) = tokio_postgres::connect(&url, tls.clone()).await?;
        mz_ore::task::spawn(|| "tokio-postgres stash connection", async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres stash connection error: {e}");
            }
        });
        client
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await?;

        let stash_factory = StashFactory::new(&MetricsRegistry::new());

        Ok(DebugStashFactory {
            url,
            schema,
            tls,
            stash_factory,
        })
    }

    /// Returns a new factory that will generate a random schema one time, then use it on any
    /// opened Stash.
    ///
    /// # Panics
    /// Panics if it is unable to create a new factory.
    pub async fn new() -> DebugStashFactory {
        DebugStashFactory::try_new()
            .await
            .expect("unable to create debug stash factory")
    }

    /// Returns a new Stash.
    pub async fn try_open_debug(&self) -> Result<Stash, StashError> {
        self.stash_factory
            .open(
                self.url.clone(),
                Some(self.schema.clone()),
                self.tls.clone(),
            )
            .await
    }

    /// Returns a new Stash.
    ///
    /// # Panics
    /// Panics if it is unable to create a new stash.
    pub async fn open_debug(&self) -> Stash {
        self.try_open_debug()
            .await
            .expect("unable to open debug stash")
    }
}

impl Drop for DebugStashFactory {
    fn drop(&mut self) {
        let url = self.url.clone();
        let schema = self.schema.clone();
        let tls = self.tls.clone();
        let result = std::thread::spawn(move || {
            let async_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            async_runtime.block_on(async {
                let (client, connection) = tokio_postgres::connect(&url, tls).await?;
                mz_ore::task::spawn(|| "tokio-postgres stash connection", async move {
                    if let Err(e) = connection.await {
                        std::panic::resume_unwind(Box::new(e));
                    }
                });
                client
                    .batch_execute(&format!("DROP SCHEMA {} CASCADE", &schema))
                    .await?;
                Ok::<_, StashError>(())
            })
        })
        // Note that we are joining on a tokio task here, which blocks the current runtime from making other progress on the current worker thread.
        // Because this only happens on shutdown and is only used in tests, we have determined that its okay
        .join();

        match result {
            Ok(result) => {
                if let Err(e) = result {
                    std::panic::resume_unwind(Box::new(e));
                }
            }

            Err(e) => std::panic::resume_unwind(e),
        }
    }
}
