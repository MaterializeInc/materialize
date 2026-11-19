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
use mz_catalog::durable::objects::DurableType;
use mz_catalog::durable::{
    CatalogError, DurableCatalogError, DurableCatalogState, Item,
    persist_backed_catalog_join_active,
};
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
        let server = test_util::TestHarness::default()
            .with_system_parameter_default("user_id_pool_batch_size".into(), "1".into())
            .start()
            .await;
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

        check_catalog_contention(&server).await;
    };
    tokio::time::timeout(Duration::from_secs(120), test_case)
        .await
        .expect("catalog completion test timed out");
}

// The failpoint makes real empty-upper conflicts after candidate construction.
// It does not synthesize an error or bypass the durable transaction boundary.
const CATALOG_FAILPOINT: &str = "catalog_before_commit";

struct RemoveCatalogHook(&'static str);

impl Drop for RemoveCatalogHook {
    fn drop(&mut self) {
        fail::remove(self.0);
    }
}

struct ReleaseCandidate(mpsc::Sender<()>);

impl Drop for ReleaseCandidate {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

async fn check_catalog_contention(server: &test_util::TestServer) {
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
        let client = complete_under_catalog_contention(
            server,
            &mut *peer,
            CatalogConflict::Progress,
            "login",
            adapter.startup(session),
        )
        .await
        .unwrap();
        drop(client);
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
    complete_under_catalog_contention(
        server,
        &mut *peer,
        CatalogConflict::Progress,
        "type commit",
        batch_execute(
            &admin,
            sql!("CREATE TYPE retry_list AS LIST (ELEMENT TYPE = int)"),
        ),
    )
    .await
    .unwrap();
    let value: i32 = query_one(&admin, sql!("SELECT (LIST[7, 8]::retry_list)[1]"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(value, 7);
    complete_under_catalog_contention(
        server,
        &mut *peer,
        CatalogConflict::IdAllocation,
        "ID pool refill",
        batch_execute(
            &admin,
            sql!("CREATE TYPE refill_list AS LIST (ELEMENT TYPE = int)"),
        ),
    )
    .await
    .unwrap();
    let value: i32 = query_one(&admin, sql!("SELECT (LIST[9, 10]::refill_list)[2]"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(value, 10);

    // A committed ID reservation does not certify the previously selected type.
    // Follow peer DDL before constructing the final catalog candidate.
    let type_id = query_one(
        &admin,
        sql!("SELECT id FROM mz_types WHERE name = 'refill_list'"),
        &[],
    )
    .await
    .unwrap()
    .get::<_, String>(0)
    .parse()
    .unwrap();
    let hook = RemoveCatalogHook("user_id_pool_after_refill");
    let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let rendezvous = Mutex::new(Some((parked_tx, resume_rx)));
    fail::cfg_callback(hook.0, move || {
        let Some((parked, resume)) = rendezvous.lock().unwrap().take() else {
            return;
        };
        tokio::task::block_in_place(|| {
            let _ = parked.send(());
            let _ = resume.recv_timeout(Duration::from_secs(180));
        });
    })
    .unwrap();
    let release = ReleaseCandidate(resume_tx);
    let drop_type = async {
        parked_rx.await.expect("TYPE committed its pool refill");
        loop {
            peer.sync_to_current_updates().await.unwrap();
            let mut txn = match peer.transaction().await {
                Ok(txn) => txn,
                Err(CatalogError::Durable(DurableCatalogError::CatalogOutOfSync { .. })) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => panic!("open peer type drop: {error}"),
            };
            // This standalone type has no comments, plans or durable dependents.
            txn.remove_item(type_id).unwrap();
            let ts = txn.upper();
            let _ = txn.get_and_commit_op_updates();
            match txn.commit(ts).await {
                Ok(()) => break,
                Err(CatalogError::Durable(DurableCatalogError::CatalogOutOfSync { .. })) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("peer type drop: {error}"),
            }
        }
        drop(release);
    };
    let creation = batch_execute(
        &admin,
        sql!("CREATE TYPE stale_list AS LIST (ELEMENT TYPE = refill_list)"),
    );
    let ((), result) = tokio::join!(drop_type, creation);
    drop(hook);
    let error = result.expect_err("allocation must not recertify a stale type plan");
    let mz_postgres_util::PostgresError::Postgres(error) = error else {
        panic!("expected SQL rejection, got {error:?}");
    };
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "type \"refill_list\" does not exist"
    );
    // A raw durable peer does not publish the derived mz_types retraction.
    // Check the authoritative state, not that separately published projection.
    peer.sync_to_current_updates().await.unwrap();
    let mut items = peer
        .snapshot()
        .await
        .unwrap()
        .items
        .into_iter()
        .map(|(key, value)| Item::from_key_value(key, value));
    assert!(!items.any(|item| matches!(item.name.as_str(), "refill_list" | "stale_list")));

    batch_execute(
        &admin,
        sql!("ALTER SYSTEM SET enable_network_policies = true"),
    )
    .await
    .unwrap();
    let policy_client = server.connect().await.unwrap();
    for statement in [
        sql!(
            "CREATE NETWORK POLICY retry_policy (RULES (r0 (address='0.0.0.0/0', action='allow', direction='ingress')))"
        ),
        sql!(
            "ALTER NETWORK POLICY retry_policy SET (RULES (r1 (address='127.0.0.1/32', action='allow', direction='ingress')))"
        ),
    ] {
        complete_under_catalog_contention(
            server,
            &mut *peer,
            CatalogConflict::Progress,
            "network policy",
            batch_execute(&policy_client, statement),
        )
        .await
        .unwrap();
    }
    let rule = query_one(
        &admin,
        sql!(
            "SELECT r.name, r.address::text FROM mz_internal.mz_network_policy_rules r
              JOIN mz_internal.mz_network_policies p ON p.id = r.policy_id
              WHERE p.name = 'retry_policy'"
        ),
        &[],
    )
    .await
    .unwrap();
    assert_eq!(rule.get::<_, String>(0), "r1");
    assert_eq!(rule.get::<_, String>(1), "127.0.0.1/32");

    let hook = RemoveCatalogHook("alter_network_policy_before_apply");
    let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let rendezvous = Mutex::new(Some((parked_tx, resume_rx)));
    fail::cfg_callback(hook.0, move || {
        let Some((parked, resume)) = rendezvous.lock().unwrap().take() else {
            return;
        };
        tokio::task::block_in_place(|| {
            let _ = parked.send(());
            let _ = resume.recv_timeout(Duration::from_secs(180));
        });
    })
    .unwrap();
    let release = ReleaseCandidate(resume_tx);
    let activate_policy = async {
        parked_rx
            .await
            .expect("ALTER reached candidate application");
        // The initial validation sees an inactive policy. A real peer then makes
        // it active before the ALTER can commit. Its stale candidate must be
        // rejected rather than replaying the earlier lockout decision.
        loop {
            peer.sync_to_current_updates().await.unwrap();
            let mut txn = match peer.transaction().await {
                Ok(txn) => txn,
                Err(CatalogError::Durable(DurableCatalogError::CatalogOutOfSync { .. })) => {
                    continue;
                }
                Err(error) => panic!("open peer transaction: {error}"),
            };
            txn.upsert_system_config("network_policy", "retry_policy".into())
                .unwrap();
            let ts = txn.upper();
            let _ = txn.get_and_commit_op_updates();
            match txn.commit(ts).await {
                Ok(()) => break,
                Err(CatalogError::Durable(DurableCatalogError::CatalogOutOfSync { .. })) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("activate policy: {error}"),
            }
        }
        drop(release);
    };
    let alteration = batch_execute(
        &policy_client,
        sql!(
            "ALTER NETWORK POLICY retry_policy SET (RULES (r1 (address='1.1.1.1/32', action='allow', direction='ingress')))"
        ),
    );
    let ((), result) = tokio::join!(activate_policy, alteration);
    drop(hook);
    let error = result.expect_err("structural conflict must reject the stale candidate");
    let mz_postgres_util::PostgresError::Postgres(error) = error else {
        panic!("expected SQL rejection, got {error:?}");
    };
    let error = error.as_db_error().unwrap();
    assert_eq!(
        error.code(),
        &tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE
    );
    assert_eq!(
        error.message(),
        "another session modified the catalog while this DDL transaction was open"
    );
    // Structural conflicts during a submitted attempt keep their existing SQL
    // error contract. A fresh statement must validate against the active policy.
    let error = batch_execute(
        &policy_client,
        sql!("ALTER NETWORK POLICY retry_policy SET (RULES (r1 (address='1.1.1.1/32', action='allow', direction='ingress')))"),
    ).await.expect_err("active policy must not lock out its invoking connection");
    let mz_postgres_util::PostgresError::Postgres(error) = error else {
        panic!("expected SQL rejection, got {error:?}");
    };
    assert!(
        error
            .as_db_error()
            .unwrap()
            .message()
            .contains("policy would block current session IP"),
        "unexpected SQL rejection: {error:?}"
    );
    let preserved = query_one(
        &admin,
        sql!(
            "SELECT r.name, r.address::text FROM mz_internal.mz_network_policy_rules r
              JOIN mz_internal.mz_network_policies p ON p.id = r.policy_id
              WHERE p.name = 'retry_policy'"
        ),
        &[],
    )
    .await
    .unwrap();
    assert_eq!(preserved.get::<_, String>(0), rule.get::<_, String>(0));
    assert_eq!(preserved.get::<_, String>(1), rule.get::<_, String>(1));
    peer.expire().await;
}

enum CatalogConflict {
    Progress,
    IdAllocation,
}

impl CatalogConflict {
    fn failpoint(&self) -> &'static str {
        match self {
            Self::Progress => CATALOG_FAILPOINT,
            Self::IdAllocation => "catalog_allocate_id_before_commit",
        }
    }

    async fn publish(&self, peer: &mut dyn DurableCatalogState) {
        match self {
            Self::Progress => {
                peer.sync_to_current_updates().await.unwrap();
                let upper = peer.current_upper().await;
                peer.advance_upper(upper.step_forward()).await.unwrap();
            }
            Self::IdAllocation => loop {
                peer.sync_to_current_updates().await.unwrap();
                let mut txn = match peer.transaction().await {
                    Ok(txn) => txn,
                    Err(CatalogError::Durable(DurableCatalogError::CatalogOutOfSync {
                        ..
                    })) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    Err(error) => panic!("open peer allocation: {error}"),
                };
                txn.get_and_increment_id_by(mz_catalog::durable::USER_ITEM_ALLOC_KEY.into(), 1)
                    .unwrap();
                let ts = txn.upper();
                let _ = txn.get_and_commit_op_updates();
                match txn.commit(ts).await {
                    Ok(()) => break,
                    Err(CatalogError::Durable(DurableCatalogError::CatalogOutOfSync {
                        ..
                    })) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("peer allocation: {error}"),
                }
            },
        }
    }
}

/// Defeat prepared catalog candidates with real peer progress while checking that
/// the coordinator still serves unrelated commands. The caller checks the result.
async fn complete_under_catalog_contention<T>(
    server: &test_util::TestServer,
    peer: &mut dyn DurableCatalogState,
    conflict: CatalogConflict,
    name: &str,
    operation: impl std::future::Future<Output = T>,
) -> T {
    let (candidate_tx, mut candidates) = tokio::sync::mpsc::unbounded_channel();
    let hook = RemoveCatalogHook(conflict.failpoint());
    fail::cfg_callback(hook.0, move || {
        let (resume_tx, resume_rx) = mpsc::channel();
        if candidate_tx.send(resume_tx).is_ok() {
            tokio::task::block_in_place(|| {
                // Dropping the driver releases queued and active candidates on
                // timeout or panic. This fuse is only a last resort for teardown.
                let _ = resume_rx.recv_timeout(Duration::from_secs(180));
            });
        }
    })
    .unwrap();
    let conflicts = AtomicUsize::new(0);
    let interference = async {
        while let Some(resume) = candidates.recv().await {
            let release = ReleaseCandidate(resume);
            conflict.publish(peer).await;
            conflicts.fetch_add(1, Ordering::SeqCst);
            drop(release);
        }
    };
    let frontend = async {
        let mut operation = Box::pin(operation);
        assert!(futures::poll!(&mut operation).is_pending());
        tokio::time::timeout(Duration::from_secs(30), async {
            while conflicts.load(Ordering::SeqCst) < 4 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("operation did not encounter peer contention");
        assert!(futures::poll!(&mut operation).is_pending());

        let responsive = tokio::time::timeout(
            Duration::from_secs(5),
            server.inner.adapter_client().get_system_vars(),
        )
        .await;
        drop(hook);
        let result = tokio::time::timeout(Duration::from_secs(30), operation)
            .await
            .expect("operation did not recover after contention");
        assert!(
            responsive.is_ok(),
            "{name} contention monopolized the coordinator"
        );
        result
    };
    let ((), result) = tokio::join!(interference, frontend);
    result
}
