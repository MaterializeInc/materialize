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
use mz_postgres_util::{Sql, batch_execute, query_one, sql};

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
    for statement in [
        sql!("CREATE VIEW retry_view AS SELECT 17 AS n"),
        sql!("CREATE OR REPLACE VIEW retry_view AS SELECT 19 AS n"),
    ] {
        complete_under_catalog_contention(
            server,
            &mut *peer,
            CatalogConflict::Progress,
            "view commit",
            batch_execute(&admin, statement),
        )
        .await
        .unwrap();
    }
    batch_execute(
        &admin,
        sql!("CREATE VIEW IF NOT EXISTS retry_view AS SELECT 99 AS n"),
    )
    .await
    .unwrap();
    let value: i32 = query_one(&admin, sql!("SELECT n FROM retry_view"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(value, 19);

    complete_under_catalog_contention(
        server,
        &mut *peer,
        CatalogConflict::IdAllocation,
        "view ID pool refill",
        batch_execute(&admin, sql!("CREATE VIEW refill_view AS SELECT 23 AS n")),
    )
    .await
    .unwrap();
    let value: i32 = query_one(&admin, sql!("SELECT n FROM refill_view"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(value, 23);

    batch_execute(&admin, sql!("BEGIN READ ONLY"))
        .await
        .unwrap();
    let explanation = complete_under_catalog_contention(
        server,
        &mut *peer,
        CatalogConflict::IdAllocation,
        "EXPLAIN ID pool refill",
        query_one(
            &admin,
            sql!("EXPLAIN LOCALLY OPTIMIZED PLAN FOR CREATE VIEW explained_view AS SELECT 31 AS n"),
            &[],
        ),
    )
    .await
    .unwrap();
    assert!(explanation.get::<_, String>(0).contains("Constant"));
    batch_execute(&admin, sql!("COMMIT")).await.unwrap();
    peer.sync_to_current_updates().await.unwrap();
    assert!(
        !peer
            .snapshot()
            .await
            .unwrap()
            .items
            .into_iter()
            .map(|(key, value)| Item::from_key_value(key, value))
            .any(|item| item.name == "explained_view")
    );

    batch_execute(&admin, sql!("INSERT INTO existing_table VALUES (41), (43)"))
        .await
        .unwrap();
    complete_under_catalog_contention(
        server,
        &mut *peer,
        CatalogConflict::IdAllocation,
        "index ID pool refill",
        batch_execute(
            &admin,
            sql!("CREATE INDEX refill_index ON existing_table (a)"),
        ),
    )
    .await
    .unwrap();
    let sum: i64 = query_one(&admin, sql!("SELECT sum(a) FROM existing_table"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(sum, 84);
    let indexes: i64 = query_one(
        &admin,
        sql!("SELECT count(*) FROM mz_indexes WHERE name = 'refill_index'"),
        &[],
    )
    .await
    .unwrap()
    .get(0);
    assert_eq!(indexes, 1);

    batch_execute(
        &admin,
        sql!("CREATE TYPE index_input_list AS LIST (ELEMENT TYPE = int)"),
    )
    .await
    .unwrap();
    let reservations = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&reservations);
    let hook = RemoveCatalogHook("user_id_pool_after_refill");
    fail::cfg_callback(hook.0, move || {
        observed.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    let explanation = query_one(
        &admin,
        sql!("EXPLAIN OPTIMIZED PLAN FOR CREATE INDEX explained_index ON existing_table ((LIST[a])::index_input_list)"),
        &[],
    ).await.unwrap();
    drop(hook);
    assert!(explanation.get::<_, String>(0).contains("existing_table"));
    assert_eq!(
        reservations.load(Ordering::SeqCst),
        0,
        "INDEX EXPLAIN must remain transient"
    );
    peer.sync_to_current_updates().await.unwrap();
    assert!(
        !peer
            .snapshot()
            .await
            .unwrap()
            .items
            .into_iter()
            .map(|(key, value)| Item::from_key_value(key, value))
            .any(|item| item.name == "explained_index")
    );
    reject_stale_definition_after_refill(
        &admin,
        &mut *peer,
        "index_input_list",
        "stale_index",
        sql!("CREATE INDEX stale_index ON existing_table ((LIST[a])::index_input_list)"),
    )
    .await;

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

    reject_stale_definition_after_refill(
        &admin,
        &mut *peer,
        "refill_list",
        "stale_list",
        sql!("CREATE TYPE stale_list AS LIST (ELEMENT TYPE = refill_list)"),
    )
    .await;
    batch_execute(
        &admin,
        sql!("CREATE TYPE view_input_list AS LIST (ELEMENT TYPE = int)"),
    )
    .await
    .unwrap();
    reject_stale_definition_after_refill(
        &admin,
        &mut *peer,
        "view_input_list",
        "stale_view",
        sql!("CREATE VIEW stale_view AS SELECT LIST[1]::view_input_list AS n"),
    )
    .await;

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
    for (hook, type_name, setup, creation, waiter) in [
        (
            "create_index_optimization_complete",
            "index_revision_bump",
            sql!("CREATE TYPE index_revision_bump AS LIST (ELEMENT TYPE = int)"),
            sql!("CREATE INDEX admission_index ON existing_table (a)"),
            sql!("CREATE ROLE index_admission_waiter"),
        ),
        (
            "create_view_optimization_complete",
            "view_revision_bump",
            sql!("CREATE TYPE view_revision_bump AS LIST (ELEMENT TYPE = int)"),
            sql!("CREATE VIEW admission_view AS SELECT 17 AS n"),
            sql!("CREATE ROLE view_admission_waiter"),
        ),
    ] {
        yield_after_invalidated_optimization(
            server, &mut *peer, hook, type_name, setup, creation, waiter,
        )
        .await;
    }
    peer.expire().await;
}

/// The named type must have no durable dependents or auxiliary metadata.
async fn reject_stale_definition_after_refill(
    admin: &tokio_postgres::Client,
    peer: &mut dyn DurableCatalogState,
    type_name: &str,
    attempted_name: &str,
    statement: Sql,
) {
    // A committed ID reservation does not certify the previously selected type.
    // Follow peer DDL before constructing the final catalog candidate.
    let type_id = query_one(
        admin,
        sql!("SELECT id FROM mz_types WHERE name = $1"),
        &[&type_name],
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
        parked_rx
            .await
            .expect("definition committed its pool refill");
        drop_standalone_type(peer, type_id).await;
        drop(release);
    };
    let creation = batch_execute(admin, statement);
    let ((), result) = tokio::join!(drop_type, creation);
    drop(hook);
    let error = result.expect_err("allocation must not recertify a stale definition");
    let mz_postgres_util::PostgresError::Postgres(error) = error else {
        panic!("expected SQL rejection, got {error:?}");
    };
    assert_eq!(
        error.as_db_error().unwrap().message(),
        format!("type \"{type_name}\" does not exist")
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
    assert!(!items.any(|item| item.name == type_name || item.name == attempted_name));
}

/// The type must have no durable dependents or auxiliary metadata. This raw
/// writer does not publish the separately derived mz_types retraction.
async fn drop_standalone_type(peer: &mut dyn DurableCatalogState, type_id: mz_repr::CatalogItemId) {
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
}

async fn yield_after_invalidated_optimization(
    server: &test_util::TestServer,
    peer: &mut dyn DurableCatalogState,
    optimization_hook: &'static str,
    type_name: &str,
    setup: Sql,
    creation: Sql,
    waiter: Sql,
) {
    let creator = server.connect().await.unwrap();
    let waiter_client = server.connect().await.unwrap();
    let observer = server.connect().await.unwrap();
    batch_execute(&creator, setup).await.unwrap();
    let type_id = query_one(
        &creator,
        sql!("SELECT id FROM mz_types WHERE name = $1"),
        &[&type_name],
    )
    .await
    .unwrap()
    .get::<_, String>(0)
    .parse()
    .unwrap();

    let admissions = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&admissions);
    let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (replanned_tx, replanned_rx) = tokio::sync::oneshot::channel();
    let first = Mutex::new(Some((parked_tx, resume_rx)));
    let hook = RemoveCatalogHook(optimization_hook);
    fail::cfg_callback(hook.0, move || {
        let pending = first.lock().unwrap().take();
        if let Some((parked, resume)) = pending {
            // The optimizer already runs on a blocking thread.
            let _ = parked.send(());
            let _ = resume.recv_timeout(Duration::from_secs(180));
        }
    })
    .unwrap();
    let release = ReleaseCandidate(resume_tx);
    let control = async move {
        parked_rx
            .await
            .expect("optimizer did not finish its first preparation");
        let adapter = server.inner.adapter_client();
        let revision = adapter
            .catalog_snapshot_expensive()
            .await
            .transient_revision();
        let (queued_tx, queued_rx) = tokio::sync::oneshot::channel();
        let queued = Mutex::new(Some(queued_tx));
        let enqueue_hook = RemoveCatalogHook("ddl_statement_enqueued");
        fail::cfg_callback(enqueue_hook.0, move || {
            let pending = queued.lock().unwrap().take();
            if let Some(queued) = pending {
                let _ = queued.send(());
            }
        })
        .unwrap();
        // Check before allocation can conflict and release admission itself.
        // Otherwise allocator backoff could mask a missing Finish release.
        let allocation_hook = RemoveCatalogHook("user_id_pool_before_allocate");
        let replanned = Mutex::new(Some(replanned_tx));
        fail::cfg_callback(allocation_hook.0, move || {
            let pending = replanned.lock().unwrap().take();
            if let Some(replanned) = pending {
                let _ = replanned.send(observed.load(Ordering::SeqCst) > 0);
            }
        })
        .unwrap();
        let admission_hook = RemoveCatalogHook("ddl_statement_dequeued");
        fail::cfg_callback(admission_hook.0, move || {
            admissions.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();

        let exercise = async {
            queued_rx.await.expect("waiter did not join the DDL queue");
            drop(enqueue_hook);
            drop_standalone_type(peer, type_id).await;
            // Certification catches up the coordinator, not only a detached
            // SQL snapshot. None of the creator's dependencies have changed.
            query_one(&observer, sql!("SELECT 1"), &[]).await.unwrap();
            let current = adapter.catalog_snapshot_expensive().await;
            assert!(current.try_get_entry(&type_id).is_none());
            assert_ne!(current.transient_revision(), revision);
            drop(release);
            assert!(
                replanned_rx
                    .await
                    .expect("creator did not restart allocation"),
                "{optimization_hook}: invalidated optimization bypassed queued DDL",
            );
            drop(allocation_hook);
            drop(admission_hook);
        };
        // Admission, rather than acknowledgement ordering, is the contract.
        // A legitimate write conflict may make the waiter yield its own turn.
        let ((), result) = tokio::join!(exercise, batch_execute(&waiter_client, waiter));
        result
    };
    tokio::try_join!(control, batch_execute(&creator, creation)).unwrap();
    drop(hook);
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
async fn complete_under_catalog_contention<T, E: std::fmt::Debug>(
    server: &test_util::TestServer,
    peer: &mut dyn DurableCatalogState,
    conflict: CatalogConflict,
    name: &str,
    operation: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E> {
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
        // Extended-protocol operations must keep polling through preparation
        // before they can submit the execution that encounters contention.
        tokio::select! {
            result = &mut operation => {
                panic!("{name} completed before peer contention: {:?}", result.err());
            }
            result = tokio::time::timeout(Duration::from_secs(30), async {
                while conflicts.load(Ordering::SeqCst) < 4 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }) => {
                result.unwrap_or_else(|error| {
                    panic!("{name} did not encounter peer contention: {error}");
                });
            }
        }
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
