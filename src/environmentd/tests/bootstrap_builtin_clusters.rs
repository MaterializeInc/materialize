// Copyright Materialize, Inc. and contributors. All rights reserved.
//G
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for builtin clusters on bootstrap.

use mz_environmentd::test_util::{self};

// Test that a cluster with a replication factor of 0 should not create any replicas.
#[mz_ore::test]
fn test_zero_replication_factor_no_replicas() {
    let server = test_util::TestHarness::default()
        .with_builtin_system_cluster_replication_factor(0)
        .start_blocking();

    let mut client = server.connect(postgres::NoTls).unwrap();
    let system_cluster = client
        .query_one(
            r#"
    SELECT c.id, c.name, c.replication_factor::integer, COUNT(cr.id)::integer as replica_count
    FROM mz_clusters c
    LEFT JOIN mz_cluster_replicas cr ON c.id = cr.cluster_id
    WHERE c.name = 'mz_system'
    GROUP BY c.id, c.name, c.replication_factor"#,
            &[],
        )
        .unwrap();

    let replication_factor: i32 = system_cluster.get(2);
    let replica_count: i32 = system_cluster.get(3);
    assert_eq!(replication_factor, 0);
    assert_eq!(replica_count, 0);
}

/// Fetch `(mz_clusters.replication_factor, count(mz_cluster_replicas))` for a
/// builtin cluster name.
fn fetch_rf_and_replica_count(
    server: &test_util::TestServerWithRuntime,
    cluster_name: &str,
) -> (i32, i32) {
    let mut client = server.connect(postgres::NoTls).unwrap();
    let row = client
        .query_one(
            "SELECT c.replication_factor::integer, \
                    COUNT(cr.id)::integer AS replica_count \
             FROM mz_clusters c \
             LEFT JOIN mz_cluster_replicas cr ON c.id = cr.cluster_id \
             WHERE c.name = $1 \
             GROUP BY c.id, c.replication_factor",
            &[&cluster_name],
        )
        .unwrap();
    (row.get(0), row.get(1))
}

// Regression test for SQL-207. The bootstrap flag seeds the durable
// `mz_clusters.replication_factor` on first boot and is not consulted
// again on subsequent boots. Whatever the flag is set to on later boots,
// the stored `replication_factor` and the actual replica count must stay
// in sync.
//
// NOTE: `TestHarness::default()` generates a fresh `environment_id` per
// call, which points each boot at a different durable catalog. Clone the
// harness and swap the flag with `with_builtin_system_cluster_replication_factor`
// so both boots share one durable catalog.
#[mz_ore::test]
fn test_builtin_system_cluster_replication_factor_zero_across_restart() {
    let data_dir = tempfile::tempdir().unwrap();
    let base = test_util::TestHarness::default().data_directory(data_dir.path());

    // First boot at rf=0 seeds the durable value.
    {
        let server = base
            .clone()
            .with_builtin_system_cluster_replication_factor(0)
            .start_blocking();
        assert_eq!(fetch_rf_and_replica_count(&server, "mz_system"), (0, 0));
    }

    // Second boot at rf=1: bootstrap flag is ignored, durable value stays
    // at 0, no replica is created.
    {
        let server = base
            .clone()
            .with_builtin_system_cluster_replication_factor(1)
            .start_blocking();
        assert_eq!(fetch_rf_and_replica_count(&server, "mz_system"), (0, 0));
    }
}

#[mz_ore::test]
fn test_builtin_system_cluster_replication_factor_one_across_restart() {
    let data_dir = tempfile::tempdir().unwrap();
    let base = test_util::TestHarness::default().data_directory(data_dir.path());

    // First boot at rf=1 seeds the durable value and creates the replica.
    {
        let server = base
            .clone()
            .with_builtin_system_cluster_replication_factor(1)
            .start_blocking();
        assert_eq!(fetch_rf_and_replica_count(&server, "mz_system"), (1, 1));
    }

    // Second boot at rf=0: bootstrap flag is ignored, durable value stays
    // at 1, replica is preserved.
    {
        let server = base
            .clone()
            .with_builtin_system_cluster_replication_factor(0)
            .start_blocking();
        assert_eq!(fetch_rf_and_replica_count(&server, "mz_system"), (1, 1));
    }
}

// `ALTER CLUSTER mz_system SET (REPLICATION FACTOR N)` must survive a
// restart. The replica migration reconciles `mz_cluster_replicas` to the
// stored `replication_factor` on boot, in both directions.
#[mz_ore::test]
fn test_builtin_system_cluster_alter_survives_restart() {
    let data_dir = tempfile::tempdir().unwrap();
    let base = test_util::TestHarness::default().data_directory(data_dir.path());

    {
        let server = base
            .clone()
            .with_builtin_system_cluster_replication_factor(1)
            .start_blocking();
        assert_eq!(fetch_rf_and_replica_count(&server, "mz_system"), (1, 1));

        let mut client = server.connect_internal(postgres::NoTls).unwrap();
        client
            .execute("ALTER CLUSTER mz_system SET (REPLICATION FACTOR 0)", &[])
            .unwrap();
        assert_eq!(fetch_rf_and_replica_count(&server, "mz_system"), (0, 0));
    }

    {
        let server = base
            .clone()
            .with_builtin_system_cluster_replication_factor(1)
            .start_blocking();
        assert_eq!(fetch_rf_and_replica_count(&server, "mz_system"), (0, 0));

        let mut client = server.connect_internal(postgres::NoTls).unwrap();
        client
            .execute("ALTER CLUSTER mz_system SET (REPLICATION FACTOR 1)", &[])
            .unwrap();
        assert_eq!(fetch_rf_and_replica_count(&server, "mz_system"), (1, 1));
    }

    {
        let server = base
            .clone()
            .with_builtin_system_cluster_replication_factor(0)
            .start_blocking();
        assert_eq!(fetch_rf_and_replica_count(&server, "mz_system"), (1, 1));
    }
}
