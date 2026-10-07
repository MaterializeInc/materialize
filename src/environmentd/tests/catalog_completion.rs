// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog acknowledgement ordering against the group committer. Keep this as
//! a single-test integration binary: the failpoint is process-global.

#![recursion_limit = "256"]

use std::sync::{Mutex, mpsc};
use std::time::Duration;

use mz_environmentd::test_util;
use mz_ore::retry::Retry;
use mz_postgres_util::{batch_execute, query_one, sql};

const FAILPOINT: &str = "group_commit_before_apply_write";

struct ReleaseCommitter(mpsc::Sender<()>);

impl Drop for ReleaseCommitter {
    fn drop(&mut self) {
        fail::remove(FAILPOINT);
        // Nonblocking, including if an assertion fails before the callback runs.
        let _ = self.0.send(());
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_catalog_completion() {
    let test_case = async {
        let server = test_util::TestHarness::default().start().await;
        let client = server.connect().await.unwrap();
        let table_client = server.connect().await.unwrap();
        batch_execute(&client, sql!("CREATE TABLE existing_table (a int)"))
            .await
            .unwrap();

        let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        // Declared after the server so unwinding releases the worker before
        // server teardown. Unlike a Barrier, this also works without rendezvous.
        let release = ReleaseCommitter(resume_tx);
        let rendezvous = Mutex::new(Some((parked_tx, resume_rx)));
        fail::cfg_callback(FAILPOINT, move || {
            let Some((parked, resume)) = rendezvous.lock().unwrap().take() else {
                return;
            };
            // Hand off the Tokio worker's queue before parking, as catalog
            // transactions still need the runtime and its shared Consensus pool.
            tokio::task::block_in_place(|| {
                let _ = parked.send(());
                // A final fuse, longer than the test deadline. Normal and panic
                // paths both release via the guard, not this timeout.
                let _ = resume.recv_timeout(Duration::from_secs(180));
            });
        })
        .unwrap();

        // Any committer write, including a periodic keepalive, can park here.
        // No assertion depends on which write it is or how many follow it.
        tokio::time::timeout(Duration::from_secs(30), parked_rx)
            .await
            .expect("committer did not reach failpoint")
            .unwrap();

        // Roles produce no builtin rows. Their acknowledgement must not need
        // the committer, even though the empty append is still staged.
        tokio::time::timeout(
            Duration::from_secs(30),
            batch_execute(&client, sql!("CREATE ROLE completion_role")),
        )
        .await
        .expect("CREATE ROLE waited for the parked committer")
        .unwrap();

        let labels = [("session_type", "user"), ("statement_type", "create_table")];
        let creates_before =
            test_util::get_counter_value(&server.metrics_registry, "mz_query_total", &labels);
        let create_table = batch_execute(&table_client, sql!("CREATE TABLE new_table (a int)"));
        tokio::pin!(create_table);
        // Confirm server-side execution has started rather than merely checking
        // an unpolled client future. This is not an exact query-count contract.
        tokio::select! {
            result = &mut create_table => {
                panic!("CREATE TABLE completed before committer release: {result:?}");
            }
            _ = async {
                Retry::default()
                    .max_duration(Duration::from_secs(30))
                    .retry_async(|_| async {
                        let creates = test_util::get_counter_value(
                            &server.metrics_registry, "mz_query_total", &labels,
                        );
                        if creates > creates_before {
                            Ok(())
                        } else {
                            Err("CREATE TABLE has not started")
                        }
                    })
                    .await
                    .unwrap();
            } => {}
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut create_table)
                .await
                .is_err(),
            "CREATE TABLE must wait for committer release"
        );

        drop(release);
        tokio::time::timeout(Duration::from_secs(30), &mut create_table)
            .await
            .expect("CREATE TABLE did not complete after committer release")
            .unwrap();

        // Both user tables and the new table's builtin column rows are readable
        // after release. This checks eventual progress, not a freshness deadline.
        for statement in [
            sql!("SELECT count(*) FROM existing_table"),
            sql!("SELECT count(*) FROM new_table"),
        ] {
            let row = query_one(&client, statement, &[]).await.unwrap();
            assert_eq!(row.get::<_, i64>(0), 0);
        }
        let row = query_one(
            &client,
            sql!(
                "SELECT c.name FROM mz_catalog.mz_columns c \
                 JOIN mz_catalog.mz_tables t ON c.id = t.id \
                 WHERE t.name = 'new_table'"
            ),
            &[],
        )
        .await
        .unwrap();
        assert_eq!(row.get::<_, String>(0), "a");
    };
    tokio::time::timeout(Duration::from_secs(120), test_case)
        .await
        .expect("catalog completion test timed out");
}
