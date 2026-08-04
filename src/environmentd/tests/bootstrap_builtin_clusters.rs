// Copyright Materialize, Inc. and contributors. All rights reserved.
//G
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for builtin clusters on bootstrap.
//!
//! A builtin cluster is a managed cluster, so its `replication_factor` is the
//! single source of truth for its replica set. These tests pin that down from
//! both directions: the factor a deployment bootstraps with is realized, and a
//! factor an operator sets with `ALTER CLUSTER` survives a restart.

use mz_environmentd::test_util::{self, TestHarness, TestServerWithRuntime};

/// A builtin cluster's declared replication factor and how many replicas it
/// actually has.
///
/// Always read over the public port, whose session cluster is `quickstart`, so a
/// test that drops `mz_system`'s replicas can still observe the result.
fn declared_and_actual(server: &TestServerWithRuntime, cluster: &str) -> (i32, i32) {
    let mut client = server.connect(postgres::NoTls).unwrap();
    let row = client
        .query_one(
            "SELECT c.replication_factor::integer,
                    (SELECT COUNT(*) FROM mz_cluster_replicas r WHERE r.cluster_id = c.id)::integer
             FROM mz_clusters c
             WHERE c.name = $1",
            &[&cluster],
        )
        .unwrap();
    (row.get(0), row.get(1))
}

/// Runs `stmt` as `user` over the internal port, which is where the system roles
/// that own the builtin clusters can connect.
fn execute_as(server: &TestServerWithRuntime, user: &str, stmt: &str) {
    let mut config = server.pg_config_internal();
    config.user(user);
    let mut client = config.connect(postgres::NoTls).unwrap();
    // The `Sql` composition wrappers are async and only defined for
    // tokio-postgres clients, and these statements are fixed test literals.
    #[allow(clippy::disallowed_methods)]
    client.batch_execute(stmt).unwrap();
}

/// A cluster's replica names, sorted.
///
/// Stronger than a count when the point is which replicas survived, and usable on
/// an unmanaged cluster, where `replication_factor` is null.
fn replica_names(server: &TestServerWithRuntime, cluster: &str) -> Vec<String> {
    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .query(
            "SELECT r.name FROM mz_cluster_replicas r
             JOIN mz_clusters c ON c.id = r.cluster_id
             WHERE c.name = $1 ORDER BY r.name",
            &[&cluster],
        )
        .unwrap()
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect()
}

// A cluster with a replication factor of 0 should not create any replicas.
#[mz_ore::test]
fn test_zero_replication_factor_no_replicas() {
    let server = test_util::TestHarness::default()
        .with_builtin_system_cluster_replication_factor(0)
        .start_blocking();

    assert_eq!(declared_and_actual(&server, "mz_system"), (0, 0));
}

// A replication factor above one is realized, not truncated to a single replica.
// The bootstrap flags accept 0..=2, and the replica set is derived from the
// factor, so asking for two replicas has to produce two.
#[mz_ore::test]
fn test_replication_factor_above_one_is_realized() {
    let server = test_util::TestHarness::default()
        .with_builtin_system_cluster_replication_factor(2)
        .start_blocking();

    assert_eq!(declared_and_actual(&server, "mz_system"), (2, 2));
}

// Scaling a builtin cluster to zero replicas sticks across a restart. The
// bootstrap flag seeds the factor at creation and must not resurrect a replica
// the operator removed.
#[mz_ore::test]
fn test_alter_to_zero_replicas_survives_restart() {
    let data_dir = tempfile::tempdir().unwrap();
    let harness = TestHarness::default().data_directory(data_dir.path());

    {
        let server = harness.clone().start_blocking();
        assert_eq!(declared_and_actual(&server, "mz_system"), (1, 1));
        execute_as(
            &server,
            "mz_system",
            "ALTER CLUSTER mz_system SET (REPLICATION FACTOR 0)",
        );
        assert_eq!(declared_and_actual(&server, "mz_system"), (0, 0));
    }

    let server = harness.start_blocking();
    assert_eq!(declared_and_actual(&server, "mz_system"), (0, 0));
}

// Scaling a builtin cluster above one replica sticks across a restart.
#[mz_ore::test]
fn test_alter_above_one_replica_survives_restart() {
    let data_dir = tempfile::tempdir().unwrap();
    let harness = TestHarness::default().data_directory(data_dir.path());

    {
        let server = harness.clone().start_blocking();
        execute_as(
            &server,
            "mz_system",
            "ALTER CLUSTER mz_system SET (REPLICATION FACTOR 2)",
        );
        assert_eq!(declared_and_actual(&server, "mz_system"), (2, 2));
    }

    let server = harness.start_blocking();
    assert_eq!(declared_and_actual(&server, "mz_system"), (2, 2));
}

// A bootstrap flag that disagrees with an existing cluster's row does not act on
// it. This is the path a helm deployment takes, since the chart defaults
// `defaultReplicationFactor.system` to 0: install at 0, then restart with the
// flag back at its default of 1. The row has to win, or the cluster ends up at
// factor 0 with a live replica, which wedges every later factor `ALTER` on the
// `(cluster_id, name)` uniqueness constraint.
#[mz_ore::test]
fn test_bootstrap_flag_does_not_act_on_existing_cluster() {
    let data_dir = tempfile::tempdir().unwrap();
    let harness = TestHarness::default().data_directory(data_dir.path());

    {
        let server = harness
            .clone()
            .with_builtin_system_cluster_replication_factor(0)
            .start_blocking();
        assert_eq!(declared_and_actual(&server, "mz_system"), (0, 0));
    }

    // Restarting without the flag leaves it at its default of 1, which must not
    // resurrect a replica. Each boot is scoped so the previous one is shut down
    // before the next opens the catalog, otherwise the older instance is
    // epoch-fenced while still running.
    {
        let server = harness.clone().start_blocking();
        assert_eq!(declared_and_actual(&server, "mz_system"), (0, 0));
    }

    // Nor does raising it further.
    let server = harness
        .with_builtin_system_cluster_replication_factor(2)
        .start_blocking();
    assert_eq!(declared_and_actual(&server, "mz_system"), (0, 0));
}

// `mz_support` defaults to zero replicas and stays that way, but a support
// engineer who scales it up keeps the replica across a restart. This is the
// break-glass path: `mz_support` runs nothing until someone needs it.
#[mz_ore::test]
fn test_support_cluster_replica_survives_restart() {
    let data_dir = tempfile::tempdir().unwrap();
    let harness = TestHarness::default().data_directory(data_dir.path());

    {
        let server = harness.clone().start_blocking();
        assert_eq!(declared_and_actual(&server, "mz_support"), (0, 0));
        execute_as(
            &server,
            "mz_support",
            "ALTER CLUSTER mz_support SET (REPLICATION FACTOR 1)",
        );
        assert_eq!(declared_and_actual(&server, "mz_support"), (1, 1));
    }

    let server = harness.start_blocking();
    assert_eq!(declared_and_actual(&server, "mz_support"), (1, 1));
}

// A replica the reconciler creates is owned by its cluster's owner, not by
// `mz_system`. `mz_support` and `mz_analytics` are owned by their own roles, and
// replica ownership is checked against the replica's own `owner_id`, so stamping
// `mz_system` here would lock those roles out of a replica of a cluster they own.
//
// Bootstrapping `mz_support` above zero also pins that its replication-factor flag
// reaches the replica set at all, which is only true because the flag seeds the
// cluster's factor and the factor is what the replica set is derived from.
#[mz_ore::test]
fn test_created_replica_is_owned_by_its_cluster_owner() {
    let server = TestHarness::default()
        .with_builtin_support_cluster_replication_factor(1)
        .start_blocking();

    assert_eq!(declared_and_actual(&server, "mz_support"), (1, 1));

    let mut client = server.connect(postgres::NoTls).unwrap();
    let owner: String = client
        .query_one(
            "SELECT o.name FROM mz_cluster_replicas r
             JOIN mz_clusters c ON c.id = r.cluster_id
             JOIN mz_roles o ON o.id = r.owner_id
             WHERE c.name = 'mz_support'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(owner, "mz_support");
}

// A restart must not disturb a builtin cluster whose replica set already matches
// its factor. Two consecutive boots leave the same replica ids in place, so
// nothing is torn down and recreated behind the operator's back.
#[mz_ore::test]
fn test_restart_is_idempotent() {
    let data_dir = tempfile::tempdir().unwrap();
    let harness = TestHarness::default().data_directory(data_dir.path());

    let replica_ids = |server: &TestServerWithRuntime| -> Vec<String> {
        let mut client = server.connect(postgres::NoTls).unwrap();
        client
            .query(
                "SELECT r.id FROM mz_cluster_replicas r
                 JOIN mz_clusters c ON c.id = r.cluster_id
                 WHERE c.id LIKE 's%' ORDER BY r.id",
                &[],
            )
            .unwrap()
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect()
    };

    let before = {
        let server = harness.clone().start_blocking();
        let before = replica_ids(&server);
        assert!(!before.is_empty(), "expected some builtin replicas");
        before
    };

    let server = harness.start_blocking();
    assert_eq!(replica_ids(&server), before);
}

// An internal replica on a builtin cluster survives a restart. `CREATE CLUSTER
// REPLICA ... INTERNAL` is allowed on a managed cluster, and its name is barred
// from matching the derived `r1..rN` pattern, so it coexists with the derived
// replicas instead of being one of them. It is the break-glass way to attach a
// replica to a builtin cluster, and `ALTER CLUSTER` already preserves it, so
// reconciling has to leave it alone too while still converging the derived set.
#[mz_ore::test]
fn test_internal_replica_survives_restart() {
    let data_dir = tempfile::tempdir().unwrap();
    let harness = TestHarness::default().data_directory(data_dir.path());

    {
        let server = harness.clone().start_blocking();
        assert_eq!(declared_and_actual(&server, "mz_system"), (1, 1));
        execute_as(
            &server,
            "mz_system",
            "CREATE CLUSTER REPLICA mz_system.breakglass SIZE 'scale=1,workers=1', INTERNAL",
        );
        // An internal replica is not derived from the factor, so it adds a replica
        // without changing what the cluster declares.
        assert_eq!(declared_and_actual(&server, "mz_system"), (1, 2));
    }

    let server = harness.start_blocking();
    assert_eq!(declared_and_actual(&server, "mz_system"), (1, 2));
    assert_eq!(
        replica_names(&server, "mz_system"),
        vec!["breakglass".to_string(), "r1".to_string()]
    );
}

// An unmanaged builtin cluster's replica set belongs to whoever made it. There is
// no replication factor to derive a target from, so reconciling has to skip the
// cluster rather than treat all of its replicas as surplus.
#[mz_ore::test]
fn test_unmanaged_builtin_cluster_is_left_alone() {
    let data_dir = tempfile::tempdir().unwrap();
    let harness = TestHarness::default()
        .with_builtin_support_cluster_replication_factor(1)
        .data_directory(data_dir.path());

    {
        let server = harness.clone().start_blocking();
        assert_eq!(declared_and_actual(&server, "mz_support"), (1, 1));
        // The conversion adopts the existing `r1`. Adding a second replica then
        // makes the set one that no replication factor would produce.
        //
        // As `mz_support`, not `mz_system`: altering a cluster requires being its
        // owner, and `mz_support` owns this one.
        execute_as(
            &server,
            "mz_support",
            "ALTER CLUSTER mz_support SET (MANAGED = false)",
        );
        execute_as(
            &server,
            "mz_support",
            "CREATE CLUSTER REPLICA mz_support.extra SIZE 'scale=1,workers=1'",
        );
        assert_eq!(
            replica_names(&server, "mz_support"),
            vec!["extra".to_string(), "r1".to_string()]
        );
    }

    let server = harness.start_blocking();
    assert_eq!(
        replica_names(&server, "mz_support"),
        vec!["extra".to_string(), "r1".to_string()]
    );
}
