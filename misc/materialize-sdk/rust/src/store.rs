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

//! The checkpoint stores the SDK keeps for a sink that cannot store its own
//! checkpoint.

use std::future::Future;

use materialize_sdk_core::CheckpointWrite;
use materialize_sdk_core::catalog::ObjectName;
use tokio_postgres::error::SqlState;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::{Client, Config, Socket};

use crate::live::{AbortOnDrop, message_with_sources};
use crate::sink::{SinkError, WriteResult};

/// Where the SDK keeps the checkpoint of a sink whose
/// [`crate::Sink::load`] returns [`SinkError::NotSupported`].
///
/// Every write is a compare-and-set on the whole checkpoint text, with the
/// results of [`crate::Sink::commit`].
pub trait CheckpointStore: Send {
    /// Creates the sink's first checkpoint, only if it has none. Returns not
    /// applied if one exists.
    fn create(
        &mut self,
        sink_id: &str,
        checkpoint: &str,
    ) -> impl Future<Output = WriteResult> + Send;

    /// The latest applied checkpoint, or `None` if the sink was never set up.
    fn load(
        &mut self,
        sink_id: &str,
    ) -> impl Future<Output = Result<Option<String>, SinkError>> + Send;

    /// The largest checkpoint text the store holds, at most
    /// [`materialize_sdk_core::checkpoint::MAX_BYTES`].
    fn max_checkpoint_bytes(&self) -> usize {
        materialize_sdk_core::checkpoint::MAX_BYTES
    }

    /// Replaces the expected checkpoint with the new one, only if the stored
    /// one is still the expected one.
    fn compare_and_set(
        &mut self,
        sink_id: &str,
        write: &CheckpointWrite,
    ) -> impl Future<Output = WriteResult> + Send;
}

/// The statement that creates the table of a [`MaterializeStore`], which the
/// setup step runs once.
pub fn checkpoint_table_sql(table: &ObjectName) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (sink_id text NOT NULL, checkpoint text NOT NULL)",
        qualified(table)
    )
}

/// A checkpoint store in a Materialize table with a `sink_id` and a
/// `checkpoint` text column, one row per sink.
///
/// Materialize runs an `UPDATE` as an optimistic read-then-write that retries
/// when the table changed under it, so an `UPDATE` conditional on the stored
/// text is a compare-and-set. The checkpoint commits apart from the target's
/// writes, so a sink that uses this store is at least once.
#[derive(Debug)]
pub struct MaterializeStore<T> {
    config: Config,
    tls: T,
    table: String,
    /// The session, reopened after it closes or after a call on it was
    /// dropped, so that a write whose response was lost can still be settled.
    session: Option<(Client, AbortOnDrop)>,
    /// Whether a statement is in flight. A call the SDK dropped leaves it set,
    /// and its statement can still hold the session.
    busy: bool,
}

impl<T> MaterializeStore<T>
where
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    T::Stream: Send + 'static,
    T::TlsConnect: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    /// Connects to Materialize with `config` and `tls` and uses `table`, which
    /// [`checkpoint_table_sql`] created.
    ///
    /// Must run in a Tokio runtime with IO enabled.
    pub async fn connect(
        config: &Config,
        tls: T,
        table: &ObjectName,
    ) -> Result<MaterializeStore<T>, SinkError> {
        let mut store = MaterializeStore {
            config: config.clone(),
            tls,
            table: qualified(table),
            session: None,
            busy: false,
        };
        store.reconnect_if_stale().await?;
        Ok(store)
    }

    /// Opens a new session if there is none, if it closed, or if a dropped
    /// call left a statement in flight on it.
    async fn reconnect_if_stale(&mut self) -> Result<(), SinkError> {
        let closed = self
            .session
            .as_ref()
            .is_none_or(|(client, _)| client.is_closed());
        if self.busy || closed {
            let (client, connection) =
                self.config
                    .connect(self.tls.clone())
                    .await
                    .map_err(|error| SinkError::Target {
                        message: message_with_sources(&error),
                        permanent: false,
                    })?;
            let connection = AbortOnDrop(tokio::spawn(async move {
                let _ = connection.await;
            }));
            self.session = Some((client, connection));
            self.busy = false;
        }
        Ok(())
    }

    /// Runs `statement` on the session, marked in flight until it returns.
    async fn on_session<R>(
        &mut self,
        statement: impl AsyncFnOnce(&Client) -> Result<R, tokio_postgres::Error>,
    ) -> Result<Result<R, tokio_postgres::Error>, SinkError> {
        self.reconnect_if_stale().await?;
        let Some((client, _)) = &self.session else {
            return Err(SinkError::Target {
                message: "no session".into(),
                permanent: false,
            });
        };
        self.busy = true;
        let result = statement(client).await;
        self.busy = false;
        Ok(result)
    }
}

impl<T> CheckpointStore for MaterializeStore<T>
where
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    T::Stream: Send + 'static,
    T::TlsConnect: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn create(&mut self, sink_id: &str, checkpoint: &str) -> WriteResult {
        let sql = format!(
            "INSERT INTO {table} SELECT $1::text, $2::text \
             WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE sink_id = $1::text)",
            table = self.table
        );
        let ran = self
            .on_session(async |client| client.execute(&sql, &[&sink_id, &checkpoint]).await)
            .await;
        match ran {
            Ok(result) => outcome(result),
            Err(error) => WriteResult::Failed(error),
        }
    }

    async fn load(&mut self, sink_id: &str) -> Result<Option<String>, SinkError> {
        let sql = format!("SELECT checkpoint FROM {} WHERE sink_id = $1", self.table);
        let rows = self
            .on_session(async |client| client.query(&sql, &[&sink_id]).await)
            .await?
            .map_err(|error| SinkError::Target {
                message: message_with_sources(&error),
                permanent: error.code().is_some_and(permanent),
            })?;
        match rows.as_slice() {
            [] => Ok(None),
            [row] => row.try_get(0).map(Some).map_err(|error| SinkError::Target {
                message: format!("the checkpoint column is not text: {error}"),
                permanent: true,
            }),
            _ => Err(SinkError::Target {
                message: format!(
                    "the checkpoint table holds {} rows for one sink",
                    rows.len()
                ),
                permanent: true,
            }),
        }
    }

    async fn compare_and_set(&mut self, sink_id: &str, write: &CheckpointWrite) -> WriteResult {
        // NOTE: Materialize counts an `UPDATE` that leaves the row unchanged
        // as zero rows, even when its condition holds. So a write that only
        // checks the condition reads instead.
        let ran = if write.expected == write.new {
            let sql = format!(
                "SELECT 1 FROM {} WHERE sink_id = $1 AND checkpoint = $2",
                self.table
            );
            self.on_session(async |client| {
                let rows = client.query(&sql, &[&sink_id, &write.expected]).await?;
                Ok(u64::try_from(rows.len()).unwrap_or(u64::MAX))
            })
            .await
        } else {
            let sql = format!(
                "UPDATE {} SET checkpoint = $3 WHERE sink_id = $1 AND checkpoint = $2",
                self.table
            );
            self.on_session(async |client| {
                client
                    .execute(&sql, &[&sink_id, &write.expected, &write.new])
                    .await
            })
            .await
        };
        match ran {
            Ok(result) => outcome(result),
            Err(error) => WriteResult::Failed(error),
        }
    }
}

/// The result of a conditional statement that changes at most one row.
fn outcome(result: Result<u64, tokio_postgres::Error>) -> WriteResult {
    match result {
        Ok(1) => WriteResult::applied(),
        Ok(0) => WriteResult::NotApplied,
        Ok(rows) => WriteResult::Failed(SinkError::Target {
            message: format!("the checkpoint table holds {rows} rows for one sink"),
            permanent: true,
        }),
        // A statement refused for its data, its SQL, or its credentials did
        // not apply. Any other error, such as Materialize's "write outcome is
        // indeterminate", or a lost connection, leaves the result unknown.
        Err(error) if error.code().is_some_and(refused) => WriteResult::Failed(SinkError::Target {
            message: message_with_sources(&error),
            permanent: error.code().is_some_and(permanent),
        }),
        Err(_) => WriteResult::Unknown,
    }
}

/// Whether a statement that failed with `code` certainly did not apply.
fn refused(code: &SqlState) -> bool {
    ["22", "23", "28", "42"]
        .iter()
        .any(|class| code.code().starts_with(class))
}

/// Whether a statement that failed with `code` fails the same way every time:
/// bad data or SQL, such as a missing table. Refused credentials can recover.
fn permanent(code: &SqlState) -> bool {
    ["22", "42"]
        .iter()
        .any(|class| code.code().starts_with(class))
}

fn qualified(table: &ObjectName) -> String {
    let quote = |ident: &str| format!("\"{}\"", ident.replace('"', "\"\""));
    let mut parts = Vec::new();
    if let Some(database) = &table.database {
        parts.push(quote(database));
    }
    parts.push(quote(&table.schema));
    parts.push(quote(&table.name));
    parts.join(".")
}
