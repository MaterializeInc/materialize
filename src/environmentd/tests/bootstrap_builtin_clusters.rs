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
