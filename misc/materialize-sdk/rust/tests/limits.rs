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

//! Subscriptions that exceed a server limit, against a running Materialize at
//! `MATERIALIZE_URL`, with `MATERIALIZE_SYSTEM_URL` to lower the limits. Run
//! with `cargo test --test limits -- --ignored`.
//!
//! The limits are system settings, so these tests run in their own test binary
//! and one at a time. Cargo runs test binaries one after another.

// The repository's clippy rules ask for wrappers a standalone SDK cannot use.
#![allow(clippy::disallowed_methods)]

mod common;

use std::time::Duration;

use common::{Cursor, STEP, Target, admin, config, connect, execute, system_config, table, value};
use materialize_sdk::core::Start;
use materialize_sdk::{Classification, ErrorKind, Subscribe};
use tokio::sync::{Mutex, MutexGuard};

static SYSTEM: Mutex<()> = Mutex::const_new(());

/// A lowered system setting, reset on drop, also when a test fails.
struct Lowered {
    name: &'static str,
    _lock: MutexGuard<'static, ()>,
}

async fn lower(name: &'static str, value: &str) -> Lowered {
    let lock = SYSTEM.lock().await;
    let system = connect(&system_config()).await;
    execute(&system, &format!("ALTER SYSTEM SET {name} = {value}")).await;
    Lowered { name, _lock: lock }
}

impl Drop for Lowered {
    fn drop(&mut self) {
        let name = self.name;
        // `drop` cannot await, and the test's current-thread runtime cannot
        // drive the reset while `drop` blocks it, so the reset gets its own.
        let reset = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let system = connect(&system_config()).await;
                execute(&system, &format!("ALTER SYSTEM RESET {name}")).await;
            });
        });
        // A failed reset must not panic again while a failing test unwinds.
        let _ = reset.join();
    }
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL and MATERIALIZE_SYSTEM_URL"]
async fn a_client_that_stops_fetching_falls_behind() {
    let admin = admin().await;
    let relation = "materialize.public.limit_behind";
    let object = table(&admin, "limit_behind", "(id int)").await;
    // Every queued message counts at least 1 KiB, so the 32 writes below
    // exceed this, and the few progress messages queued before the first
    // FETCH do not.
    let _lowered = lower("subscribe_max_buffered_bytes", "16384").await;
    let subscribed = format!(
        "SELECT count(*)::text FROM mz_internal.mz_subscriptions \
         WHERE '{}' = ANY(referenced_object_ids)",
        object.object.id
    );
    let subscribe = Subscribe::object(object.object);
    let start = Start::Snapshot { as_of: None };
    let mut cursor = Cursor::open(&config(), subscribe, start).await.unwrap();
    let mut target = Target::default();
    cursor
        .fetch_until(&mut target, |t| t.frontier.is_some())
        .await
        .unwrap();
    for id in 0..32 {
        execute(&admin, &format!("INSERT INTO {relation} VALUES ({id})")).await;
    }
    // The server checks the backlog as each response arrives, and retires the
    // subscribe once it is over the budget.
    let deadline = tokio::time::Instant::now() + STEP;
    while value(&admin, &subscribed).await != "0" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the subscribe was not retired"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let failure = cursor.drain(&mut target).await.unwrap_err();
    assert_eq!(failure.sqlstate.as_deref(), Some("53200"), "{failure:?}");
    assert_eq!(
        failure.classification,
        Classification::Kind {
            kind: ErrorKind::FellBehind
        }
    );
    drop(cursor);
    execute(&admin, &format!("DROP TABLE {relation}")).await;
}

#[tokio::test]
#[ignore = "needs a running Materialize at MATERIALIZE_URL and MATERIALIZE_SYSTEM_URL"]
async fn a_transaction_over_max_result_size_is_too_large() {
    let admin = admin().await;
    let relation = "materialize.public.limit_large";
    let object = table(&admin, "limit_large", "(v text)").await;
    // The smallest value the setting accepts.
    let _lowered = lower("max_result_size", "'1MB'").await;
    let subscribe = Subscribe::object(object.object);
    let start = Start::Snapshot { as_of: None };
    let mut cursor = Cursor::open(&config(), subscribe, start).await.unwrap();
    let mut target = Target::default();
    cursor
        .fetch_until(&mut target, |t| t.frontier.is_some())
        .await
        .unwrap();
    // About 3 MB at one timestamp, in distinct rows that do not consolidate.
    execute(
        &admin,
        &format!(
            "INSERT INTO {relation} \
             SELECT repeat('x', 1000) || g FROM generate_series(1, 3000) AS g"
        ),
    )
    .await;
    let failure = cursor.drain(&mut target).await.unwrap_err();
    assert!(
        failure.message.contains("result exceeds max size of"),
        "{failure:?}"
    );
    assert_eq!(
        failure.classification,
        Classification::Kind {
            kind: ErrorKind::ResultTooLarge
        }
    );
    drop(cursor);
    execute(&admin, &format!("DROP TABLE {relation}")).await;
}
