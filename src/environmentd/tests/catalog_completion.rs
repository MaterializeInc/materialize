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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use mz_adapter::session::SessionConfig;
use mz_auth::{Authenticated, AuthenticatorKind};
use mz_catalog::durable::persist_backed_catalog_join_active;
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

        check_startup_contention(&server).await;
    };
    tokio::time::timeout(Duration::from_secs(120), test_case)
        .await
        .expect("catalog completion test timed out");
}

// The failpoint makes real empty-upper conflicts after candidate construction.
// It does not synthesize an error or bypass the durable transaction boundary.
const CATALOG_FAILPOINT: &str = "catalog_before_commit";

struct RemoveCatalogHook;

impl Drop for RemoveCatalogHook {
    fn drop(&mut self) {
        fail::remove(CATALOG_FAILPOINT);
    }
}

struct ReleaseCandidate(mpsc::Sender<()>);

impl Drop for ReleaseCandidate {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

async fn check_startup_contention(server: &test_util::TestServer) {
    let admin = server.connect().internal().await.unwrap();
    for statement in [
        sql!("ALTER SYSTEM SET oidc_group_role_sync_enabled = true"),
        sql!("CREATE ROLE login_group"),
        sql!("CREATE ROLE manual_group"),
    ] {
        batch_execute(&admin, statement).await.unwrap();
    }
    let persist = server
        .persist_clients
        .open(server.persist_location.clone())
        .await
        .unwrap();
    let mut peer = persist_backed_catalog_join_active(
        persist,
        server.environment_id.organization_id(),
        mz_environmentd::BUILD_INFO.semver_version(),
        Arc::new(mz_catalog::durable::Metrics::new(
            &mz_ore::metrics::MetricsRegistry::new(),
        )),
        Some(server.catalog_timestamp_oracle().await),
    )
    .await
    .unwrap();
    let adapter = server.inner.adapter_client();

    // First force auto-provisioning to yield, then force membership synchronization
    // for the same role. Neither retry may register a session before completion.
    for groups in [
        None,
        Some(vec!["login_group".to_owned(), "manual_group".to_owned()]),
    ] {
        if groups.is_some() {
            // Establish this before synchronization, never repair it afterward.
            batch_execute(&admin, sql!("GRANT manual_group TO contention_login"))
                .await
                .unwrap();
        }
        let session = adapter.new_session(
            SessionConfig {
                conn_id: adapter.new_conn_id().unwrap(),
                uuid: uuid::Uuid::new_v4(),
                user: "contention_login".to_owned(),
                client_ip: Some(std::net::Ipv4Addr::LOCALHOST.into()),
                external_metadata_rx: None,
                helm_chart_version: None,
                authenticator_kind: AuthenticatorKind::None,
                groups,
            },
            Authenticated,
        );
        let (candidate_tx, mut candidates) = tokio::sync::mpsc::unbounded_channel();
        let hook = RemoveCatalogHook;
        fail::cfg_callback(CATALOG_FAILPOINT, move || {
            let (resume_tx, resume_rx) = mpsc::channel();
            if candidate_tx.send(resume_tx).is_ok() {
                tokio::task::block_in_place(|| {
                    // Panic/timeout drops the receiver and the per-attempt release
                    // guard. The fuse is only a last resort for test teardown.
                    let _ = resume_rx.recv_timeout(Duration::from_secs(180));
                });
            }
        })
        .unwrap();
        let conflicts = AtomicUsize::new(0);
        let interference = async {
            while let Some(resume) = candidates.recv().await {
                let release = ReleaseCandidate(resume);
                peer.sync_to_current_updates().await.unwrap();
                let upper = peer.current_upper().await;
                peer.advance_upper(upper.step_forward()).await.unwrap();
                conflicts.fetch_add(1, Ordering::SeqCst);
                drop(release);
            }
        };
        let frontend = async {
            let mut startup = Box::pin(adapter.startup(session));
            assert!(futures::poll!(&mut startup).is_pending());
            tokio::time::timeout(Duration::from_secs(30), async {
                while conflicts.load(Ordering::SeqCst) < 4 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("startup did not encounter peer contention");
            assert!(futures::poll!(&mut startup).is_pending());

            // This round trip must complete while the peer is still defeating
            // every catalog write, not only after the login finally commits.
            let responsive =
                tokio::time::timeout(Duration::from_secs(5), adapter.get_system_vars()).await;
            drop(hook);
            let client = tokio::time::timeout(Duration::from_secs(30), &mut startup)
                .await
                .expect("startup did not recover after contention")
                .unwrap();
            assert!(
                responsive.is_ok(),
                "login contention monopolized the coordinator"
            );
            drop(client);
        };
        tokio::join!(interference, frontend);
    }
    let rows = mz_postgres_util::query(
        &admin,
        sql!(
            "SELECT r.name, g.name FROM mz_role_members m
              JOIN mz_roles r ON m.role_id = r.id
              JOIN mz_roles u ON m.member = u.id
              JOIN mz_roles g ON m.grantor = g.id
              WHERE u.name = 'contention_login' ORDER BY r.name"
        ),
        &[],
    )
    .await
    .unwrap();
    let memberships: Vec<(String, String)> =
        rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    assert_eq!(
        memberships,
        vec![
            ("login_group".into(), "mz_jwt_sync".into()),
            ("manual_group".into(), "mz_system".into()),
        ]
    );
    peer.expire().await;
}
