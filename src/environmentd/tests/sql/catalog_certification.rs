// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

use mz_environmentd::test_util::TestHarness;
use mz_postgres_util::{batch_execute, query_prepared, simple_query, sql};
use serde_json::json;

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn preparation_and_execution_share_one_catalog() {
    let server = TestHarness::default()
        .with_system_parameter_default("enable_frontend_peek_sequencing".into(), "true".into())
        .start()
        .await;
    let client = server.connect().await.unwrap();
    for statement in [
        sql!("CREATE TABLE certification_input (a int)"),
        sql!("INSERT INTO certification_input VALUES (1)"),
        sql!("CREATE DEFAULT INDEX ON certification_input"),
        sql!("CREATE ROLE anonymous_http_user WITH LOGIN"),
        sql!("GRANT SELECT ON certification_input TO anonymous_http_user"),
    ] {
        batch_execute(&client, statement).await.unwrap();
    }
    let snapshots = |context: &str| -> f64 {
        server
            .metrics_registry
            .gather()
            .iter()
            .filter(|family| family.name() == "mz_catalog_snapshot_cache")
            .flat_map(|family| family.get_metric())
            .filter(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == "context" && label.value() == context)
            })
            .map(|metric| metric.get_counter().value())
            .sum()
    };
    for isolation in [
        sql!("SET transaction_isolation = 'strict serializable'"),
        sql!("SET transaction_isolation = 'serializable'"),
    ] {
        batch_execute(&client, isolation).await.unwrap();
        let declared = snapshots("declare");
        let peeked = snapshots("try_frontend_peek");
        for _ in 0..3 {
            let rows = simple_query(&client, sql!("SELECT * FROM certification_input"))
                .await
                .unwrap();
            assert!(rows.iter().any(|row| matches!(row,
                tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0) == Some("1"))));
        }
        assert_eq!(snapshots("declare") - declared, 3.0);
        assert_eq!(snapshots("try_frontend_peek") - peeked, 0.0);
    }

    let statement = client
        .prepare("SELECT * FROM certification_input")
        .await
        .unwrap();
    let verified = snapshots("get_prepared_statement");
    let peeked = snapshots("try_frontend_peek");
    let rows = query_prepared(&client, &statement, &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(snapshots("get_prepared_statement") - verified, 1.0);
    assert_eq!(snapshots("try_frontend_peek") - peeked, 1.0);

    let prepared = snapshots("prepare");
    let verified = snapshots("get_prepared_statement");
    let peeked = snapshots("try_frontend_peek");
    let response: serde_json::Value = reqwest::Client::new()
        .post(format!("http://{}/api/sql", server.http_local_addr()))
        .json(&json!({"query": "SELECT * FROM certification_input"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        response["results"][0]["rows"].as_array().map(Vec::len),
        Some(1),
        "{response}"
    );
    assert_eq!(snapshots("prepare") - prepared, 1.0);
    assert_eq!(snapshots("get_prepared_statement") - verified, 0.0);
    assert_eq!(snapshots("try_frontend_peek") - peeked, 0.0);

    batch_execute(
        &client,
        sql!("REVOKE SELECT ON certification_input FROM anonymous_http_user"),
    )
    .await
    .unwrap();
    let response: serde_json::Value = reqwest::Client::new()
        .post(format!("http://{}/api/sql", server.http_local_addr()))
        .json(&json!({"query": "SELECT * FROM certification_input"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        response["results"][0]["error"]["code"], "42501",
        "{response}"
    );
}
