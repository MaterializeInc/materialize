// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Keeps a Postgres table equal to a Materialize table, source, or
//! materialized view.
//!
//! Each batch of changes and the checkpoint commit in one Postgres
//! transaction. Once the first snapshot completes, the target is always the
//! object at the committed frontier, and a restart resumes from there. While
//! a snapshot is written, its rows so far are visible next to the previous
//! one's.
//!
//! ```text
//! MATERIALIZE_URL="host=localhost port=6875 user=materialize" \
//! POSTGRES_URL="host=localhost port=5432 user=postgres" \
//! SOURCE=materialize.public.orders \
//! cargo run --example postgres_sink
//! ```
//!
//! `SINK_ID` names the checkpoint, `postgres-example` by default. The rows
//! land in `mz_rows` as JSON arrays of each column's text, with their count.

// The repository's clippy rules ask for `mz_ore` wrappers around
// `tokio::spawn`, which a standalone SDK cannot depend on.
#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;

use materialize_sdk::{
    Change, CheckpointWrite, DurableOptions, DurableSink, Envelope, ObjectName, Sink, SinkBatch,
    SinkError, SinkType, WriteKind, WriteRequest, WriteResult,
};
use openssl::ssl::{SslConnector, SslFiletype, SslMethod};
use openssl::x509::store::X509Lookup;
use postgres_openssl::MakeTlsConnector;
use tokio_postgres::{Client, Config, IsolationLevel, Transaction};

/// The rows of every sink that writes here, keyed by a hash of the row so
/// that a wide row fits the index, and the checkpoint of each.
const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS mz_rows (
        sink_id text NOT NULL,
        generation bigint NOT NULL,
        row_hash text NOT NULL,
        row jsonb NOT NULL,
        count bigint NOT NULL,
        PRIMARY KEY (sink_id, generation, row_hash)
    );
    CREATE TABLE IF NOT EXISTS mz_checkpoints (
        sink_id text PRIMARY KEY,
        checkpoint text NOT NULL
    );
";

/// Adds each row's count, or sets it for a snapshot chunk, which can arrive
/// again after a lost response and holds each row once with its full count.
const UPSERT_ROWS: &str = "
    INSERT INTO mz_rows
    SELECT $1, $2, encode(sha256(convert_to(r, 'UTF8')), 'hex'), r::jsonb, c
    FROM unnest($3::text[], $4::bigint[]) AS u(r, c)
    ON CONFLICT (sink_id, generation, row_hash) DO UPDATE SET count =
        CASE WHEN $5 THEN EXCLUDED.count ELSE mz_rows.count + EXCLUDED.count END
";

const DELETE_EMPTY_ROWS: &str = "
    DELETE FROM mz_rows
    WHERE sink_id = $1 AND generation = $2 AND count = 0
      AND row_hash IN (
          SELECT encode(sha256(convert_to(r, 'UTF8')), 'hex') FROM unnest($3::text[]) AS u(r)
      )
";

const DELETE_OLDER_GENERATIONS: &str = "DELETE FROM mz_rows WHERE sink_id = $1 AND generation < $2";

/// How many rows one statement carries, so that a large batch stays below
/// Postgres's limit on one message.
const ROWS_PER_STATEMENT: usize = 10_000;

const REPLACE_CHECKPOINT: &str =
    "UPDATE mz_checkpoints SET checkpoint = $3 WHERE sink_id = $1 AND checkpoint = $2";

struct Postgres {
    config: Config,
    tls: MakeTlsConnector,
    /// The session, opened again once it closes, as after Postgres restarts.
    client: Option<Client>,
    sink_id: String,
}

impl Postgres {
    /// The open session. A session that cannot open sent nothing, so the call
    /// fails without an unknown result.
    async fn client(&mut self) -> Result<&mut Client, Failure> {
        if self.client.as_ref().is_none_or(Client::is_closed) {
            let (client, connection) =
                self.config
                    .connect(self.tls.clone())
                    .await
                    .map_err(|error| {
                        Failure::Refused(SinkError::Target {
                            message: format!("cannot connect to Postgres: {error}"),
                            permanent: false,
                        })
                    })?;
            tokio::spawn(async move {
                if let Err(error) = connection.await {
                    eprintln!("the connection to Postgres failed: {error}");
                }
            });
            self.client = Some(client);
        }
        self.client
            .as_mut()
            .ok_or_else(|| Failure::Lost("no session".into()))
    }

    async fn apply(
        &mut self,
        batch: &SinkBatch,
        write: &CheckpointWrite,
    ) -> Result<WriteResult, Failure> {
        let generation = bigint(batch.generation())?;
        // The net change of each row, so the batch takes one statement.
        let mut changes: BTreeMap<String, i64> = BTreeMap::new();
        for update in batch.updates() {
            if let Change::Diff { row, diff } = &update.change {
                let row =
                    serde_json::to_string(row).map_err(|e| Failure::permanent(e.to_string()))?;
                let count = changes.entry(row).or_default();
                *count = count
                    .checked_add(*diff)
                    .ok_or_else(|| Failure::permanent("a row's count overflows".into()))?;
            }
        }
        let changes: Vec<(String, i64)> = changes.into_iter().collect();
        // Read committed checks the checkpoint condition against the latest
        // committed row, and locks it until the transaction ends.
        let sink_id = self.sink_id.clone();
        let tx = self
            .client()
            .await?
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()
            .await?;
        let Some(expected) = &write.expected else {
            return Err(Failure::permanent(
                "the first checkpoint is written without a batch".into(),
            ));
        };
        let replaced = tx
            .execute(REPLACE_CHECKPOINT, &[&sink_id, expected, &write.new])
            .await?;
        if replaced == 0 {
            tx.rollback().await?;
            return Ok(WriteResult::NotApplied);
        }
        let chunk = matches!(write.kind, WriteKind::SnapshotWrite { .. });
        for piece in changes.chunks(ROWS_PER_STATEMENT) {
            let rows: Vec<&str> = piece.iter().map(|(row, _)| row.as_str()).collect();
            let counts: Vec<i64> = piece.iter().map(|(_, count)| *count).collect();
            tx.execute(
                UPSERT_ROWS,
                &[&sink_id, &generation, &rows, &counts, &chunk],
            )
            .await?;
            tx.execute(DELETE_EMPTY_ROWS, &[&sink_id, &generation, &rows])
                .await?;
        }
        // Readers never see an older snapshot next to the one that completes.
        if let WriteKind::CompleteSnapshot { generation, .. } = write.kind {
            Postgres::delete_older(&tx, &sink_id, generation).await?;
        }
        commit(tx).await
    }

    async fn delete_older(
        tx: &Transaction<'_>,
        sink_id: &str,
        generation: u64,
    ) -> Result<(), Failure> {
        tx.execute(DELETE_OLDER_GENERATIONS, &[&sink_id, &bigint(generation)?])
            .await?;
        Ok(())
    }

    /// Writes a checkpoint without a batch. The first one is inserted only if
    /// the sink has none.
    async fn write_checkpoint(&mut self, write: &CheckpointWrite) -> Result<WriteResult, Failure> {
        let sink_id = self.sink_id.clone();
        let client = self.client().await?;
        let written = match &write.expected {
            None => {
                client
                    .execute(
                        "INSERT INTO mz_checkpoints VALUES ($1, $2) ON CONFLICT DO NOTHING",
                        &[&sink_id, &write.new],
                    )
                    .await
            }
            Some(expected) => {
                client
                    .execute(REPLACE_CHECKPOINT, &[&sink_id, expected, &write.new])
                    .await
            }
        };
        Ok(changed(written))
    }
}

/// A lost response to `COMMIT`, or an error from it, leaves the result
/// unknown, because the commit can have applied.
async fn commit(tx: Transaction<'_>) -> Result<WriteResult, Failure> {
    match tx.commit().await {
        Ok(()) => Ok(WriteResult::applied()),
        Err(_) => Ok(WriteResult::Unknown),
    }
}

/// Why a statement failed.
enum Failure {
    /// The server refused it, so it did not apply.
    Refused(SinkError),
    /// The connection failed, so whether it applied is unknown.
    Lost(String),
}

impl Failure {
    fn permanent(message: String) -> Failure {
        Failure::Refused(SinkError::Target {
            message,
            permanent: true,
        })
    }

    fn into_result(self) -> WriteResult {
        match self {
            Failure::Refused(error) => WriteResult::Failed(error),
            Failure::Lost(_) => WriteResult::Unknown,
        }
    }
}

impl From<tokio_postgres::Error> for Failure {
    fn from(error: tokio_postgres::Error) -> Failure {
        let Some(db) = error.as_db_error() else {
            return Failure::Lost(error.to_string());
        };
        // Bad data, a constraint, bad SQL, or a row too large fails the same
        // way on every attempt.
        let class = db.code().code().get(..2).unwrap_or_default();
        Failure::Refused(SinkError::Target {
            message: format!("{} (SQLSTATE {})", db.message(), db.code().code()),
            permanent: matches!(class, "22" | "23" | "42" | "54"),
        })
    }
}

impl From<Failure> for SinkError {
    fn from(failure: Failure) -> SinkError {
        match failure {
            Failure::Refused(error) => error,
            Failure::Lost(message) => SinkError::Target {
                message,
                permanent: false,
            },
        }
    }
}

fn bigint(value: u64) -> Result<i64, Failure> {
    i64::try_from(value).map_err(|_| Failure::permanent(format!("{value} does not fit a bigint")))
}

/// The result of a statement that changes one row if it applied.
fn changed(result: Result<u64, tokio_postgres::Error>) -> WriteResult {
    match result {
        Ok(1) => WriteResult::applied(),
        Ok(_) => WriteResult::NotApplied,
        Err(error) => Failure::from(error).into_result(),
    }
}

impl Sink for Postgres {
    fn sink_type(&self) -> SinkType {
        SinkType::Transactional
    }

    async fn load(&mut self) -> Result<Option<String>, SinkError> {
        let sink_id = self.sink_id.clone();
        let row = self
            .client()
            .await?
            .query_opt(
                "SELECT checkpoint FROM mz_checkpoints WHERE sink_id = $1",
                &[&sink_id],
            )
            .await
            .map_err(Failure::from)?;
        Ok(row.map(|row| row.get(0)))
    }

    async fn write(&mut self, request: WriteRequest<'_>) -> WriteResult {
        let written = match request {
            WriteRequest::BatchAndCheckpoint(batch, write) => self.apply(batch, write).await,
            WriteRequest::Checkpoint(write) => self.write_checkpoint(write).await,
            WriteRequest::Batch(_) => {
                return WriteResult::Failed(SinkError::Target {
                    message: "a transactional sink writes with a checkpoint".into(),
                    permanent: true,
                });
            }
        };
        written.unwrap_or_else(Failure::into_result)
    }

    async fn sweep_generations(&mut self, generation: u64) -> Result<(), SinkError> {
        let generation = bigint(generation)?;
        let sink_id = self.sink_id.clone();
        self.client()
            .await?
            .execute(DELETE_OLDER_GENERATIONS, &[&sink_id, &generation])
            .await
            .map_err(Failure::from)?;
        Ok(())
    }
}

fn env(name: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(name).map_err(|_| format!("set {name}").into())
}

/// A TLS connector that trusts the system's certificate authorities, which
/// the vendored OpenSSL does not know where to find. `openssl-probe` finds
/// them on Linux and the BSDs, and macOS keeps them in `/etc/ssl/cert.pem`.
/// `SSL_CERT_FILE` adds one more. The connector verifies the server's
/// certificate, so a server with a self-signed one needs its CA there, or
/// `sslmode=disable`.
fn tls() -> Result<MakeTlsConnector, Box<dyn Error>> {
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    let probed = openssl_probe::probe();
    let macos = Path::new("/etc/ssl/cert.pem");
    let file = probed
        .cert_file
        .or_else(|| macos.exists().then(|| macos.to_path_buf()));
    if let Some(file) = file {
        builder.set_ca_file(file)?;
    }
    for dir in probed.cert_dir.iter().filter_map(|dir| dir.to_str()) {
        builder
            .cert_store_mut()
            .add_lookup(X509Lookup::hash_dir())?
            .add_dir(dir, SslFiletype::PEM)?;
    }
    Ok(MakeTlsConnector::new(builder.build()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let materialize: Config = env("MATERIALIZE_URL")?.parse()?;
    let postgres: Config = env("POSTGRES_URL")?.parse()?;
    let source = env("SOURCE")?;
    let [database, schema, name] = source.split('.').collect::<Vec<_>>()[..] else {
        return Err("SOURCE is database.schema.name".into());
    };
    let source = ObjectName {
        database: Some(database.into()),
        schema: schema.into(),
        name: name.into(),
    };
    let sink_id = std::env::var("SINK_ID").unwrap_or_else(|_| "postgres-example".into());

    // Both connections use TLS when the server offers it, as Materialize
    // Cloud does, and their URL's `sslmode` can require it.
    let tls = tls()?;
    let (client, connection) = postgres.connect(tls.clone()).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("the connection to Postgres failed: {error}");
        }
    });
    client.batch_execute(SCHEMA).await?;

    let sink = Postgres {
        config: postgres,
        tls: tls.clone(),
        client: Some(client),
        sink_id: sink_id.clone(),
    };
    let mut durable = DurableSink::new(sink_id.as_str(), sink, DurableOptions::default());
    if durable
        .setup(&materialize, tls.clone(), &source, Envelope::Diff)
        .await?
    {
        println!("created the checkpoint for {sink_id}");
    }
    println!("writing {database}.{schema}.{name} into mz_rows as {sink_id}");
    durable.run(&materialize, tls).await?;
    Ok(())
}
