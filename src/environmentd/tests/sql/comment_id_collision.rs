// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_environmentd::test_util::TestHarness;
use mz_postgres_util::{Sql, batch_execute, query, query_one, sql};
use tokio_postgres::Client;

// SQL-531: replica and item IDs have independent allocators but share the u<N>
// spelling. Comment joins must distinguish object types without hiding base rows.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn test_comment_id_collision() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let server = TestHarness::default().start().await;
        let client = server.connect().await.unwrap();
        // Freeze BOTH targets before churning replicas: native metric-sink
        // allocation consumes item IDs on each replica creation. The MV executes
        // on the existing default cluster, never on the disposable cluster.
        for statement in [
            sql!("CREATE CLUSTER c REPLICAS (), MANAGED = false"),
            sql!("CREATE TABLE t1 (a int)"),
            sql!("CREATE TABLE t2 (a int)"),
            sql!("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t1"),
        ] {
            batch_execute(&client, statement).await.unwrap();
        }
        let targets = query_one(
            &client,
            sql!("SELECT (SELECT id FROM mz_tables WHERE name = 't2'),
                         (SELECT id FROM mz_materialized_views WHERE name = 'mv')"),
            &[],
        )
        .await
        .unwrap();
        let table_id: String = targets.get(0);
        let mv_id: String = targets.get(1);
        assert!(user_id(&table_id) < user_id(&mv_id), "targets must be ascending: {table_id}, {mv_id}");

        let mut creations = 0;
        collide(&client, &table_id, &mut creations).await;
        let table_collision = sql!(
            "SELECT (SELECT r.id FROM mz_cluster_replicas r
                     JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'c')
                  = (SELECT id FROM mz_tables WHERE name = 't2')"
        );
        check_collision(&client, table_collision.clone()).await;
        // A table-only comment must not hide the uncommented replica.
        batch_execute(&client, sql!("COMMENT ON TABLE t2 IS 'boom'"))
            .await
            .unwrap();
        check(
            &client,
            sql!("SELECT cluster, replica FROM mz_internal.mz_show_cluster_replicas WHERE cluster = 'c'"),
            &[[Some("c"), Some("r1")]],
        ).await;

        // Reuse the same collision with only the replica commented.
        for statement in [
            sql!("COMMENT ON TABLE t2 IS NULL"),
            sql!("COMMENT ON CLUSTER REPLICA c.r1 IS 'replica_note'"),
        ] {
            batch_execute(&client, statement).await.unwrap();
        }
        check_collision(&client, table_collision.clone()).await;
        check_collision(&client, sql!(
            "SELECT (SELECT r.id FROM mz_cluster_replicas r
                     JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'c')
                 <> (SELECT id FROM mz_tables WHERE name = 't1')"
        )).await;
        let tables = sql!("SELECT name, type, comment FROM mz_internal.mz_show_all_objects
                           WHERE name IN ('t1', 't2') ORDER BY name");
        check(&client, tables, &[
            [Some("t1"), Some("table"), Some("")],
            [Some("t2"), Some("table"), Some("")],
        ]).await;
        let replica = sql!("SELECT replica, comment FROM mz_internal.mz_show_cluster_replicas WHERE cluster = 'c'");
        check(&client, replica.clone(), &[[Some("r1"), Some("replica_note")]]).await;

        // Both comments must be attributed to their own object, exactly once.
        batch_execute(&client, sql!("COMMENT ON TABLE t2 IS 'table_note'"))
            .await
            .unwrap();
        check_collision(&client, table_collision).await;
        check(&client, sql!("SELECT name, type, comment FROM mz_internal.mz_show_all_objects WHERE name = 't2'"),
              &[[Some("t2"), Some("table"), Some("table_note")]]).await;
        check(&client, replica, &[[Some("r1"), Some("replica_note")]]).await;

        batch_execute(&client, sql!("DROP CLUSTER REPLICA c.r1")).await.unwrap();
        collide(&client, &mv_id, &mut creations).await;
        check_collision(&client, sql!(
            "SELECT (SELECT r.id FROM mz_cluster_replicas r
                     JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'c')
                  = (SELECT id FROM mz_materialized_views WHERE name = 'mv')"
        )).await;
        batch_execute(&client, sql!("COMMENT ON CLUSTER REPLICA c.r1 IS 'replica_note'"))
            .await
            .unwrap();
        let products = sql!("SELECT object_name, description FROM mz_internal.mz_mcp_data_products
                             WHERE object_name = '\"materialize\".\"public\".\"mv\"'");
        let details = sql!("SELECT object_name, description FROM mz_internal.mz_mcp_data_product_details
                            WHERE object_name = '\"materialize\".\"public\".\"mv\"'");
        let name = Some("\"materialize\".\"public\".\"mv\"");
        for view in [products.clone(), details.clone()] {
            check(&client, view, &[[name, None]]).await;
        }
        batch_execute(&client, sql!("COMMENT ON MATERIALIZED VIEW mv IS 'mv_note'"))
            .await
            .unwrap();
        for view in [products, details] {
            check(&client, view, &[[name, Some("mv_note")]]).await;
        }
    })
    .await
    .expect("SQL-531 comment ID collision test timed out");
}

fn user_id(id: &str) -> u64 {
    id.strip_prefix('u')
        .and_then(|id| id.parse().ok())
        .unwrap_or_else(|| panic!("expected a numeric user ID, got {id:?}"))
}

// Enter with no scratch replica. Keep at most one catalog replica, and use
// observed IDs rather than assuming allocator offsets or contiguous allocation.
async fn collide(client: &Client, target: &str, creations: &mut usize) {
    loop {
        // This is a cost budget, not an allocation assumption. Exceeding it is
        // a fixture failure, never permission to skip the collision assertions.
        assert!(
            *creations < 64,
            "SQL-531 exceeded 64 replica creations chasing {target}"
        );
        batch_execute(
            client,
            sql!("CREATE CLUSTER REPLICA c.r1 SIZE 'scale=1,workers=1'"),
        )
        .await
        .unwrap();
        *creations += 1;
        let id: String = query_one(
            client,
            sql!(
                "SELECT r.id FROM mz_cluster_replicas r
             JOIN mz_clusters c ON r.cluster_id = c.id WHERE c.name = 'c'"
            ),
            &[],
        )
        .await
        .unwrap()
        .get(0);
        assert!(
            user_id(&id) <= user_id(target),
            "SQL-531 replica allocator passed frozen target {target}: got {id} after {creations} creations"
        );
        if id == target {
            return;
        }
        batch_execute(client, sql!("DROP CLUSTER REPLICA c.r1"))
            .await
            .unwrap();
    }
}

async fn check_collision(client: &Client, statement: Sql) {
    let row = query_one(client, statement, &[]).await.unwrap();
    assert!(
        row.get::<_, bool>(0),
        "SQL-531 collision prerequisite failed"
    );
}

async fn check<const N: usize>(client: &Client, statement: Sql, expected: &[[Option<&str>; N]]) {
    let rows = query(client, statement.clone(), &[]).await.unwrap();
    let actual: Vec<[Option<&str>; N]> = rows
        .iter()
        .map(|row| std::array::from_fn(|i| row.get(i)))
        .collect();
    assert_eq!(actual, expected, "{}", statement.as_str());
}
