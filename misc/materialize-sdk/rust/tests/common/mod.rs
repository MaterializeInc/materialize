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

//! Helpers for the tests against a running Materialize.

// Each test file uses a subset of these helpers.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::time::Duration;

use materialize_sdk::core::catalog::{self, CatalogObject, ObjectName};
use materialize_sdk::core::{Start, Subscription};
use materialize_sdk::{Change, Classification, Event, Frontier, Row, Subscribe};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};

/// The longest any one step may take before a test fails instead of hanging.
pub const STEP: Duration = Duration::from_secs(60);

/// The connection to Materialize, from `MATERIALIZE_URL`.
pub fn config() -> Config {
    url("MATERIALIZE_URL")
}

/// A connection as `mz_system`, from `MATERIALIZE_SYSTEM_URL`.
pub fn system_config() -> Config {
    url("MATERIALIZE_SYSTEM_URL")
}

fn url(variable: &str) -> Config {
    std::env::var(variable)
        .unwrap_or_else(|_| panic!("{variable} is not set"))
        .parse()
        .unwrap()
}

pub async fn connect(config: &Config) -> Client {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

pub async fn admin() -> Client {
    connect(&config()).await
}

pub async fn execute(client: &Client, sql: &str) {
    tokio::time::timeout(STEP, client.batch_execute(sql))
        .await
        .unwrap_or_else(|_| panic!("timed out: {sql}"))
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

pub async fn rows(client: &Client, sql: &str) -> (Vec<String>, Vec<Row>) {
    let messages = tokio::time::timeout(STEP, client.simple_query(sql))
        .await
        .unwrap_or_else(|_| panic!("timed out: {sql}"))
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    read_rows(messages)
}

fn read_rows(messages: Vec<SimpleQueryMessage>) -> (Vec<String>, Vec<Row>) {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for message in messages {
        // The description comes before the rows, and also for no rows.
        if let SimpleQueryMessage::RowDescription(description) = &message {
            columns = description.iter().map(|c| c.name().to_string()).collect();
        }
        if let SimpleQueryMessage::Row(row) = message {
            rows.push(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect(),
            );
        }
    }
    (columns, rows)
}

/// The single value `sql` returns.
pub async fn value(client: &Client, sql: &str) -> String {
    let (_, found) = rows(client, sql).await;
    match <[Row; 1]>::try_from(found) {
        Ok([row]) => match <[Option<String>; 1]>::try_from(row) {
            Ok([Some(value)]) => value,
            other => panic!("{sql} returned {other:?}"),
        },
        Err(found) => panic!("{sql} returned {found:?}"),
    }
}

/// Looks up `materialize.{schema}.{name}`.
pub async fn lookup(client: &Client, schema: &str, name: &str) -> Option<CatalogObject> {
    let sql = catalog::lookup_sql(&ObjectName {
        database: Some("materialize".into()),
        schema: schema.into(),
        name: name.into(),
    });
    let (columns, found) = rows(client, &sql).await;
    catalog::parse_lookup(&columns, found).unwrap()
}

/// Creates the table `materialize.public.{name}` from `definition`, such as
/// `(id int)`, after dropping any table of that name a failed run left behind.
pub async fn table(client: &Client, name: &str, definition: &str) -> CatalogObject {
    execute(
        client,
        &format!("DROP TABLE IF EXISTS materialize.public.{name} CASCADE"),
    )
    .await;
    execute(
        client,
        &format!("CREATE TABLE materialize.public.{name} {definition}"),
    )
    .await;
    lookup(client, "public", name).await.unwrap()
}

/// An error from the server, classified by the subscription it ended.
#[derive(Debug)]
pub struct Failure {
    pub sqlstate: Option<String>,
    pub message: String,
    pub classification: Classification,
}

/// One cursor driven through the protocol core, for the scenarios that the
/// Rust package's live subscriptions do not cover, such as a resume.
pub struct Cursor {
    client: Client,
    sub: Subscription,
    connection: JoinHandle<()>,
}

impl Cursor {
    /// Connects with `config`, then opens the transaction and declares the
    /// cursor.
    pub async fn open(
        config: &Config,
        subscribe: Subscribe,
        start: Start,
    ) -> Result<Cursor, Failure> {
        let mut sub = Subscription::new(subscribe, start).unwrap();
        let declare = sub.declare_sql().unwrap();
        let (client, connection) = config.connect(NoTls).await.unwrap();
        let connection = tokio::spawn(async move {
            let _ = connection.await;
        });
        let cursor = Cursor {
            client,
            sub,
            connection,
        };
        let begin = cursor.client.batch_execute("BEGIN");
        cursor.run("BEGIN", begin).await?;
        // The extended protocol, as the core requires of a transport.
        let declared = cursor.client.execute(declare.as_str(), &[]);
        cursor.run(&declare, declared).await?;
        Ok(cursor)
    }

    async fn run<T>(
        &self,
        sql: &str,
        request: impl Future<Output = Result<T, tokio_postgres::Error>>,
    ) -> Result<(), Failure> {
        let result = tokio::time::timeout(STEP, request)
            .await
            .unwrap_or_else(|_| panic!("timed out: {sql}"));
        result.map(|_| ()).map_err(|e| self.failure(&e))
    }

    /// Runs one `FETCH` and returns the events it releases.
    pub async fn fetch(&mut self) -> Result<Vec<Event>, Failure> {
        let fetch = self
            .sub
            .fetch(1000, Some(Duration::from_millis(500)))
            .unwrap();
        let result = tokio::time::timeout(STEP, self.client.simple_query(fetch.sql()))
            .await
            .expect("a FETCH within the step limit");
        let (columns, rows) = read_rows(result.map_err(|e| self.failure(&e))?);
        Ok(self.sub.push(fetch, &columns, rows).unwrap())
    }

    /// Fetches into `target` until `done` holds. Panics if the stream ends
    /// first.
    pub async fn fetch_until(
        &mut self,
        target: &mut Target,
        done: impl Fn(&Target) -> bool,
    ) -> Result<(), Failure> {
        let deadline = tokio::time::Instant::now() + STEP;
        while !done(target) {
            assert!(
                !self.sub.is_finished(),
                "the stream ended before the condition held"
            );
            assert!(
                tokio::time::Instant::now() < deadline,
                "the condition did not hold within the step limit"
            );
            for event in self.fetch().await? {
                target.apply(event);
            }
        }
        Ok(())
    }

    /// Fetches into `target` until the stream ends.
    pub async fn drain(&mut self, target: &mut Target) -> Result<(), Failure> {
        let deadline = tokio::time::Instant::now() + STEP;
        while !self.sub.is_finished() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the stream did not end within the step limit"
            );
            for event in self.fetch().await? {
                target.apply(event);
            }
        }
        Ok(())
    }

    pub fn is_finished(&self) -> bool {
        self.sub.is_finished()
    }

    fn failure(&self, error: &tokio_postgres::Error) -> Failure {
        let (sqlstate, message) = match error.as_db_error() {
            Some(db) => (Some(db.code().code().to_string()), db.message().to_string()),
            None => (None, error.to_string()),
        };
        let classification = self.sub.classify(sqlstate.as_deref(), &message);
        Failure {
            sqlstate,
            message,
            classification,
        }
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        // Ends the session, and with it the subscribe, as a crash would.
        self.connection.abort();
    }
}

/// A target for the diff envelope: the rows as a multiset, and the frontier
/// of the last batch applied.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Target {
    pub rows: BTreeMap<Row, i64>,
    pub frontier: Option<Frontier>,
}

impl Target {
    /// A target that holds `rows` at `frontier`, as one restored from storage.
    pub fn restored(frontier: Frontier, rows: BTreeMap<Row, i64>) -> Target {
        Target {
            rows,
            frontier: Some(frontier),
        }
    }

    pub fn apply(&mut self, event: Event) {
        let updates = match event {
            Event::SnapshotChunk { updates } => updates,
            Event::Batch(batch) => {
                assert_eq!(batch.lower(), self.frontier, "a batch continues the target");
                self.frontier = Some(batch.upper());
                batch.into_updates()
            }
        };
        for update in updates {
            let Change::Diff { row, diff } = update.change else {
                panic!("expected the diff envelope, got {update:?}");
            };
            match self.rows.entry(row) {
                Entry::Vacant(entry) => {
                    entry.insert(diff);
                }
                Entry::Occupied(mut entry) => {
                    *entry.get_mut() += diff;
                    if *entry.get() == 0 {
                        entry.remove();
                    }
                }
            }
        }
    }

    /// Whether a row starts with `cell`.
    pub fn has(&self, cell: &str) -> bool {
        self.rows
            .keys()
            .any(|row| row.first().and_then(Option::as_deref) == Some(cell))
    }

    /// Asserts that the target holds what `relation` held at `AS OF F - 1`,
    /// where `F` is the target's frontier: the state a committed frontier
    /// stands for.
    pub async fn assert_matches(&self, client: &Client, relation: &str) {
        let frontier = self.frontier.expect("a target with a frontier");
        // Computed here, so that a bug in `Frontier::resume_as_of` cannot hide.
        let as_of = u64::try_from(frontier.get() - 1).unwrap();
        let sql = format!("SELECT * FROM {relation} AS OF {as_of}");
        let mut expected: BTreeMap<Row, i64> = BTreeMap::new();
        for row in rows(client, &sql).await.1 {
            *expected.entry(row).or_default() += 1;
        }
        assert_eq!(self.rows, expected, "{relation} at {frontier}");
    }
}
