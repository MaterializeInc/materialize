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

//! Keeps a Valkey hash equal to a Materialize relation with a unique key.
//!
//! ```text
//! MATERIALIZE_URL="host=localhost port=6875 user=materialize" \
//! VALKEY_URL=redis://localhost:6379 \
//! SOURCE=orders KEY=id \
//! cargo run --example valkey
//! ```
//!
//! `SOURCE` names a table, view, source, or materialized view, and `KEY` its
//! unique key columns, separated by commas. The hash is named `HASH`, or
//! `SOURCE` when unset. Each field is a key, as a JSON array of the key
//! columns' text, and each value is the row's other columns, as a JSON object
//! of their text with `null` for SQL `NULL`. Each batch is applied in one
//! `MULTI`, so a reader sees all of a timestamp's changes or none of them.
//!
//! Each connection starts with a snapshot, because the subscription is to a
//! query, which cannot resume. The snapshot loads into `<HASH>:loading` and
//! replaces the hash with one `RENAME` once it is complete, so readers keep the
//! previous contents until then. Resuming without a snapshot would need the
//! relation's history to outlast the time disconnected (`RETAIN HISTORY`, in
//! private preview) and a durable record of the last frontier.

use std::env;
use std::time::Duration;

use materialize_sdk::core::Envelope;
use materialize_sdk::{
    Change, Classification, Error, ErrorKind, Event, LiveOptions, LiveSubscription, Subscribe,
    Update,
};
use redis::aio::MultiplexedConnection;
use serde_json::{Map, Value};

const MAX_BACKOFF: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let var = |name: &str| env::var(name).map_err(|_| format!("set {name}"));
    let config: tokio_postgres::Config = var("MATERIALIZE_URL")?.parse()?;
    let source = var("SOURCE")?;
    let key: Vec<String> = var("KEY")?.split(',').map(|c| c.trim().into()).collect();
    let hash = env::var("HASH").unwrap_or_else(|_| source.clone());
    let valkey = redis::Client::open(
        env::var("VALKEY_URL").unwrap_or_else(|_| "redis://localhost:6379".into()),
    )?;
    let subscribe = Subscribe::query(format!("SELECT * FROM {source}"))
        .with_envelope(Envelope::Upsert { key: key.clone() });

    let mut backoff = Duration::from_secs(1);
    loop {
        let failure = match sync(&config, &valkey, &subscribe, key.len(), &hash, &mut backoff).await
        {
            Ok(()) => return Ok(()),
            Err(failure) => failure,
        };
        if !failure.retry {
            return Err(failure.error);
        }
        eprintln!("reconnecting in {backoff:?}: {}", failure.error);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

struct Failure {
    error: Box<dyn std::error::Error>,
    retry: bool,
}

impl From<Error> for Failure {
    fn from(error: Error) -> Failure {
        // A new connection takes a new snapshot, which also recovers from
        // falling behind.
        let retry = match &error {
            Error::Server { classification, .. } => matches!(
                classification,
                Classification::Kind {
                    kind: ErrorKind::Transient | ErrorKind::FellBehind
                }
            ),
            Error::ConsumerFellBehind { .. } => true,
            _ => false,
        };
        Failure {
            error: error.into(),
            retry,
        }
    }
}

impl From<redis::RedisError> for Failure {
    fn from(error: redis::RedisError) -> Failure {
        Failure {
            retry: error.is_io_error() || error.is_connection_dropped(),
            error: error.into(),
        }
    }
}

/// Runs one connection to Materialize until the stream ends or fails.
async fn sync(
    config: &tokio_postgres::Config,
    valkey: &redis::Client,
    subscribe: &Subscribe,
    key_len: usize,
    hash: &str,
    backoff: &mut Duration,
) -> Result<(), Failure> {
    let mut valkey = valkey.get_multiplexed_async_connection().await?;
    let mut live = LiveSubscription::connect(
        config,
        tokio_postgres::NoTls,
        subscribe.clone(),
        LiveOptions::default(),
    )
    .await?;
    let loading = format!("{hash}:loading");
    // A connection that failed during its snapshot left a partial one here.
    let () = redis::cmd("DEL")
        .arg(&loading)
        .query_async(&mut valkey)
        .await?;
    while let Some(event) = live.next().await {
        let event = event?;
        let columns = live.columns().unwrap_or_default();
        let value_columns = columns.get(key_len..).unwrap_or_default();
        match event {
            Event::SnapshotChunk { updates } => {
                write(&mut valkey, &loading, value_columns, &updates, false).await?;
            }
            Event::Batch(batch) if batch.is_snapshot() => {
                write(&mut valkey, &loading, value_columns, batch.updates(), false).await?;
                // An empty snapshot writes no field, which leaves no hash to
                // rename.
                let exists: bool = redis::cmd("EXISTS")
                    .arg(&loading)
                    .query_async(&mut valkey)
                    .await?;
                let swap = if exists { "RENAME" } else { "DEL" };
                let mut cmd = redis::cmd(swap);
                if exists {
                    cmd.arg(&loading);
                }
                let () = cmd.arg(hash).query_async(&mut valkey).await?;
                println!("loaded the snapshot into {hash}");
                *backoff = Duration::from_secs(1);
            }
            Event::Batch(batch) => {
                write(&mut valkey, hash, value_columns, batch.updates(), true).await?;
            }
        }
    }
    Ok(())
}

/// Applies `updates` to `hash`, in one transaction if `atomic`.
async fn write(
    valkey: &mut MultiplexedConnection,
    hash: &str,
    value_columns: &[String],
    updates: &[Update],
    atomic: bool,
) -> Result<(), redis::RedisError> {
    if updates.is_empty() {
        return Ok(());
    }
    let mut pipe = redis::pipe();
    if atomic {
        pipe.atomic();
    }
    for update in updates {
        match &update.change {
            Change::Upsert { key, value } => {
                let mut object = Map::new();
                for (i, name) in value_columns.iter().enumerate() {
                    object.insert(name.clone(), value.get(i).map_or(Value::Null, json));
                }
                pipe.hset(hash, field(key), Value::Object(object).to_string())
                    .ignore();
            }
            Change::Delete { key } => {
                pipe.hdel(hash, field(key)).ignore();
            }
            // The key held more than one row at this timestamp, so it has no
            // single value to keep.
            Change::KeyViolation { key } => {
                eprintln!("the key {} is not unique, so it is removed", field(key));
                pipe.hdel(hash, field(key)).ignore();
            }
            Change::Diff { .. } => unreachable!("the upsert envelope has no diffs"),
        }
    }
    pipe.query_async(valkey).await
}

fn field(key: &[Option<String>]) -> String {
    Value::Array(key.iter().map(json).collect()).to_string()
}

fn json(datum: &Option<String>) -> Value {
    datum.clone().map_or(Value::Null, Value::String)
}
