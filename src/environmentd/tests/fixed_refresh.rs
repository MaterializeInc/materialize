// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fixed-refresh creation may cross unrelated DDL, but not dependency or privilege changes.

#![recursion_limit = "256"]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use mz_environmentd::test_util::TestHarness;
use mz_repr::AdapterId;
use tokio_postgres::error::SqlState;

const HOOK: &str = "create_materialized_view_optimization_complete";
const DEADLINE: Duration = Duration::from_secs(180);

struct Unpause(Option<mpsc::Sender<()>>);

impl Drop for Unpause {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
        fail::remove(HOOK);
    }
}

#[derive(Clone, Copy, Debug)]
enum Change {
    Unrelated,
    AlterInput,
    DropInput,
    RenameTargetSchema,
    DropTargetSchema,
    RevokeTargetCreate,
    RevokeInputUsage,
    RevokeMembership,
}

#[mz_ore::test]
#[allow(clippy::disallowed_methods)]
fn fixed_refresh_revalidation() {
    // Both the test future and runtime workers need the integration-test stack.
    std::thread::Builder::new()
        .stack_size(4 * 1024 * 1024)
        .spawn(|| {
            // One test owns the process-global hook, so cases cannot overlap.
            for change in [
                Change::Unrelated,
                Change::AlterInput,
                Change::DropInput,
                Change::RenameTargetSchema,
                Change::DropTargetSchema,
                Change::RevokeTargetCreate,
                Change::RevokeInputUsage,
                Change::RevokeMembership,
            ] {
                // Persist background tasks must end with their fixture, rather
                // than repeatedly trying to read its deleted temporary blobs.
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_stack_size(4 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        tokio::time::timeout(DEADLINE, Box::pin(run(change)))
                            .await
                            .unwrap_or_else(|_| panic!("{change:?}: fixture timed out"));
                    });
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

#[allow(clippy::disallowed_methods)]
async fn run(change: Change) {
    let harness = TestHarness::default()
        .with_system_parameter_default("enable_refresh_every_mvs".into(), "true".into())
        .with_system_parameter_default("enable_logical_compaction_window".into(), "true".into())
        .with_system_parameter_default("enable_alter_table_add_column".into(), "true".into());
    let server = harness.clone().start().await;
    let client = server.connect().internal().await.unwrap();
    let writer = server.connect().internal().await.unwrap();
    for statement in [
        "CREATE SCHEMA refresh_source",
        "CREATE TYPE refresh_source.int_list AS LIST (ELEMENT TYPE = int)",
        "CREATE TABLE refresh_source.refresh_input (a int) WITH (RETAIN HISTORY FOR '1 hour')",
        "INSERT INTO refresh_source.refresh_input VALUES (1), (2)",
        "CREATE SCHEMA refresh_target",
        "CREATE ROLE refresh_creator LOGIN",
        "CREATE ROLE refresh_creation_role",
        "GRANT USAGE ON DATABASE materialize TO refresh_creation_role",
        "GRANT USAGE, CREATE ON CLUSTER quickstart TO refresh_creation_role",
        "GRANT USAGE, CREATE ON SCHEMA refresh_target TO refresh_creation_role",
        "GRANT USAGE ON SCHEMA refresh_source TO refresh_creation_role",
        "GRANT USAGE ON TYPE refresh_source.int_list TO refresh_creation_role",
        "GRANT SELECT ON TABLE refresh_source.refresh_input TO refresh_creation_role",
        "GRANT refresh_creation_role TO refresh_creator",
        "CREATE ADAPTER refresh_peer (SIZE '1')",
    ] {
        client.batch_execute(statement).await.unwrap();
    }
    // The creator owns neither schema nor input. All required grants are
    // inherited, and the private input schema has no PUBLIC USAGE grant.
    let creator = server.connect().user("refresh_creator").await.unwrap();
    assert!(
        !creator
            .query_one("SELECT mz_is_superuser()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0),
        "creation must exercise privilege checks"
    );
    let peer_id: String = client
        .query_one(
            "SELECT id FROM mz_catalog.mz_adapters WHERE name = 'refresh_peer'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let peer = harness
        .with_adapter_id(peer_id.parse::<AdapterId>().unwrap())
        .start_in(server.deployment())
        .await;
    let peer_client = peer.connect().internal().await.unwrap();

    // Retention protects this known timestamp before CREATE acquires its own
    // protection. No read transaction stays open during the rendezvous.
    let timestamp: String = client
        .query_one(
            "SELECT mz_now()::text FROM refresh_source.refresh_input LIMIT 1",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let timestamp: u64 = timestamp.parse().unwrap();
    let snapshot =
        format!("SELECT a FROM refresh_source.refresh_input ORDER BY a AS OF {timestamp}");
    let rows = client.query(&snapshot, &[]).await.unwrap();
    assert_eq!(
        rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(),
        vec![1, 2],
        "known snapshot before optimization"
    );

    let (parked_tx, mut parked_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let rendezvous = Mutex::new(Some((parked_tx, release_rx)));
    let failed = Arc::new(AtomicBool::new(false));
    let callback_failed = Arc::clone(&failed);
    // Declared after both servers so unwinding releases the callback first.
    let mut unpause = Unpause(Some(release_tx));
    fail::cfg_callback(HOOK, move || {
        let rendezvous = rendezvous.lock().unwrap().take();
        if let Some((parked, release)) = rendezvous {
            if parked.send(()).is_err() || release.recv_timeout(Duration::from_secs(60)).is_err() {
                callback_failed.store(true, Ordering::SeqCst);
            }
        }
    })
    .unwrap();

    // MV creation requires USAGE on referenced types and their schemas, not
    // SELECT on input tables. Keep a type reference to exercise dependency USAGE.
    let sql = "CREATE MATERIALIZED VIEW refresh_target.refresh_mv WITH (REFRESH AT CREATION)
               AS SELECT (LIST[a]::refresh_source.int_list)[1] AS a
               FROM refresh_source.refresh_input";
    let creation = creator.batch_execute(sql);
    tokio::pin!(creation);
    tokio::select! {
        result = &mut parked_rx => result.expect("optimization rendezvous"),
        result = &mut creation => panic!("{change:?}: CREATE finished before rendezvous: {result:?}"),
    }

    // These committed writes must not appear in the fixed snapshot, even if
    // catalog revalidation runs after them. Repurifying AT CREATION would select
    // a newer timestamp and expose these writes.
    for statement in [
        "INSERT INTO refresh_source.refresh_input VALUES (3)",
        "DELETE FROM refresh_source.refresh_input WHERE a = 1",
    ] {
        writer
            .batch_execute(statement)
            .await
            .expect("input writes while optimization is parked");
    }
    match change {
        Change::Unrelated => {
            // CREATE SECRET bypasses same-adapter DDL admission but changes the
            // planning revision. CREATE TABLE here would queue behind CREATE MV.
            writer
                .batch_execute("CREATE SECRET refresh_unrelated AS 'synthetic-test-value'")
                .await
                .expect("unrelated structural DDL committed while parked");
        }
        Change::AlterInput
        | Change::DropInput
        | Change::RenameTargetSchema
        | Change::DropTargetSchema
        | Change::RevokeTargetCreate
        | Change::RevokeInputUsage
        | Change::RevokeMembership => {
            // A separately declared hosted adapter has independent admission.
            let ddl = match change {
                Change::AlterInput => "ALTER TABLE refresh_source.refresh_input ADD COLUMN b int",
                Change::DropInput => "DROP TABLE refresh_source.refresh_input",
                Change::RenameTargetSchema => {
                    "ALTER SCHEMA refresh_target RENAME TO renamed_target"
                }
                Change::DropTargetSchema => "DROP SCHEMA refresh_target",
                Change::RevokeTargetCreate => {
                    "REVOKE CREATE ON SCHEMA refresh_target FROM refresh_creation_role"
                }
                Change::RevokeInputUsage => {
                    "REVOKE USAGE ON SCHEMA refresh_source FROM refresh_creation_role"
                }
                Change::RevokeMembership => "REVOKE refresh_creation_role FROM refresh_creator",
                Change::Unrelated => unreachable!(),
            };
            peer_client
                .batch_execute(ddl)
                .await
                .expect("dependency or privilege mutation committed while parked");
            // Certify the first adapter after the peer commit before releasing
            // optimization, rather than relying on follower scheduling.
            let exists: bool = writer
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM mz_catalog.mz_tables WHERE name = 'refresh_input')",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(exists, !matches!(change, Change::DropInput));
            let revoked_privilege = match change {
                Change::RevokeTargetCreate => Some(
                    "SELECT has_schema_privilege('refresh_creator', 'refresh_target', 'CREATE')",
                ),
                Change::RevokeInputUsage => Some(
                    "SELECT has_schema_privilege('refresh_creator', 'refresh_source', 'USAGE')",
                ),
                Change::RevokeMembership => {
                    Some("SELECT pg_has_role('refresh_creator', 'refresh_creation_role', 'USAGE')")
                }
                _ => None,
            };
            if let Some(query) = revoked_privilege {
                let allowed: bool = writer.query_one(query, &[]).await.unwrap().get(0);
                assert!(
                    !allowed,
                    "{change:?}: revocation must be visible before release"
                );
            }
            if matches!(
                change,
                Change::RenameTargetSchema | Change::DropTargetSchema
            ) {
                let exists: bool = writer.query_one(
                    "SELECT EXISTS (SELECT 1 FROM mz_catalog.mz_schemas WHERE name = 'refresh_target')",
                    &[],
                ).await.unwrap().get(0);
                assert!(!exists, "target namespace change must be visible");
            }
        }
    }
    unpause.0.take().unwrap().send(()).unwrap();
    let result = creation.await;
    assert!(!failed.load(Ordering::SeqCst), "rendezvous failed");
    drop(unpause);

    match change {
        Change::Unrelated => {
            result.expect("unrelated DDL must not reject fixed-refresh creation");
            let rows = client
                .query("SELECT a FROM refresh_target.refresh_mv ORDER BY a", &[])
                .await
                .unwrap();
            assert_eq!(
                rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(),
                vec![1, 2],
                "fixed refresh must preserve the original timestamp"
            );
            let rows = client
                .query("SELECT a FROM refresh_source.refresh_input ORDER BY a", &[])
                .await
                .unwrap();
            assert_eq!(
                rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(),
                vec![2, 3],
                "input must have advanced beyond the fixed snapshot"
            );
        }
        Change::AlterInput
        | Change::DropInput
        | Change::RenameTargetSchema
        | Change::DropTargetSchema
        | Change::RevokeTargetCreate
        | Change::RevokeInputUsage
        | Change::RevokeMembership => {
            let error = result.expect_err("dependency or privilege change must reject creation");
            let expected = match change {
                Change::AlterInput | Change::RenameTargetSchema | Change::DropTargetSchema => {
                    SqlState::T_R_SERIALIZATION_FAILURE
                }
                Change::DropInput => SqlState::UNDEFINED_OBJECT,
                Change::RevokeTargetCreate
                | Change::RevokeInputUsage
                | Change::RevokeMembership => SqlState::INSUFFICIENT_PRIVILEGE,
                Change::Unrelated => unreachable!(),
            };
            assert_eq!(
                error.as_db_error().expect("SQL error").code(),
                &expected,
                "{change:?}: {error}"
            );
            let exists: bool = client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM mz_catalog.mz_materialized_views WHERE name = 'refresh_mv')",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            assert!(
                !exists,
                "failed CREATE must not publish a materialized view"
            );
        }
    }
}
