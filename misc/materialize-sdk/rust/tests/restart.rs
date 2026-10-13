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

//! A resume across a restart of Materialize, in two phases that the
//! `materialize-sdk` mzcompose composition runs with a restart between them.
//! The composition names the phase in `MATERIALIZE_SDK_RESTART_PHASE`, and the
//! target passes from one phase to the next in the file at
//! `MATERIALIZE_SDK_RESTART_STATE`, which the second phase removes.

// The repository's clippy rules ask for wrappers a standalone SDK cannot use.
#![allow(clippy::disallowed_methods)]

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{Cursor, Target, admin, config, execute, lookup, table, value};
use materialize_sdk::core::Start;
use materialize_sdk::{Frontier, Row, Subscribe};
use serde_json::json;

const RELATION: &str = "materialize.public.restart_target";

/// The state file, if the composition runs `phase` now.
fn state_path(phase: &str) -> String {
    let env = |name: &str| {
        std::env::var(name)
            .unwrap_or_else(|_| panic!("{name} is not set; run test/materialize-sdk/mzcompose"))
    };
    assert_eq!(env("MATERIALIZE_SDK_RESTART_PHASE"), phase);
    env("MATERIALIZE_SDK_RESTART_STATE")
}

fn since_epoch() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

#[tokio::test]
#[ignore = "run by test/materialize-sdk/mzcompose, which restarts Materialize after it"]
async fn before_a_restart() {
    let path = state_path("before");
    let admin = admin().await;
    let object = table(
        &admin,
        "restart_target",
        "(id text) WITH (RETAIN HISTORY = FOR '1h')",
    )
    .await;
    execute(
        &admin,
        &format!("INSERT INTO {RELATION} VALUES ('a'), ('b')"),
    )
    .await;
    let start = Start::Snapshot { as_of: None };
    let mut cursor = Cursor::open(&config(), Subscribe::object(object.object), start)
        .await
        .unwrap();
    let mut target = Target::default();
    cursor
        .fetch_until(&mut target, |t| t.has("a") && t.has("b"))
        .await
        .unwrap();
    drop(cursor);
    // Changes the resume after the restart has to deliver.
    execute(&admin, &format!("DELETE FROM {RELATION} WHERE id = 'a'")).await;
    execute(&admin, &format!("INSERT INTO {RELATION} VALUES ('c')")).await;
    target.assert_matches(&admin, RELATION).await;
    let rows: Vec<(Row, i64)> = target.rows.into_iter().collect();
    let state = json!({
        "frontier": target.frontier.unwrap(),
        "rows": rows,
        "written_ms": since_epoch().as_millis(),
    });
    std::fs::write(path, state.to_string()).unwrap();
}

#[tokio::test]
#[ignore = "run by test/materialize-sdk/mzcompose, after it restarts Materialize"]
async fn after_a_restart() {
    let path = state_path("after");
    let admin = admin().await;
    let state: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let uptime_ms: u128 = value(
        &admin,
        "SELECT (extract(epoch FROM mz_uptime()) * 1000)::bigint::text",
    )
    .await
    .parse()
    .unwrap();
    let written_ms = state["written_ms"].as_u64().map(u128::from).unwrap();
    assert!(
        uptime_ms < since_epoch().as_millis() - written_ms,
        "Materialize restarted after the first phase"
    );
    let frontier: Frontier = serde_json::from_value(state["frontier"].clone()).unwrap();
    let rows: Vec<(Row, i64)> = serde_json::from_value(state["rows"].clone()).unwrap();
    let mut target = Target::restored(frontier, rows.into_iter().collect());
    execute(&admin, &format!("INSERT INTO {RELATION} VALUES ('d')")).await;
    let object = lookup(&admin, "public", "restart_target").await.unwrap();
    let start = Start::Resume { frontier };
    let mut cursor = Cursor::open(&config(), Subscribe::object(object.object), start)
        .await
        .unwrap();
    cursor
        .fetch_until(&mut target, |t| t.has("c") && t.has("d") && !t.has("a"))
        .await
        .unwrap();
    target.assert_matches(&admin, RELATION).await;
    drop(cursor);
    execute(&admin, &format!("DROP TABLE {RELATION}")).await;
    std::fs::remove_file(path).unwrap();
}
