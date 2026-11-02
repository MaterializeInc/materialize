// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the schema registry's HTTP API.

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use mz_kafka_server::{
    KafkaServer, KafkaServerConfig, SaslPlainCredentials, TopicOptions, TopicRegistration,
    TopicSchemas,
};
use serde_json::{Value, json};
use tower::ServiceExt;

const SCHEMA: &str = r#"{"type": "record", "name": "row", "fields": [{"name": "id", "type": "long"}, {"name": "item", "type": "string"}]}"#;

fn register(
    server: &KafkaServer,
    topic: &str,
    credentials: Option<SaslPlainCredentials>,
    schemas: TopicSchemas,
) -> TopicRegistration {
    server
        .register_topic(
            topic.into(),
            TopicOptions {
                credentials,
                validator: None,
                schemas,
                queue_bytes: 1024 * 1024,
                compacted: Vec::new(),
            },
        )
        .unwrap()
}

/// Sends a request to the registry and returns the status and the JSON body.
async fn request(
    server: &KafkaServer,
    method: &str,
    uri: &str,
    auth: Option<(&str, &str)>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some((user, password)) = auth {
        let encoded = base64_basic(user, password);
        request = request.header(header::AUTHORIZATION, format!("Basic {encoded}"));
    }
    let body = match body {
        Some(body) => {
            request = request.header(header::CONTENT_TYPE, "application/json");
            Body::from(body.to_string())
        }
        None => Body::empty(),
    };
    let response = server
        .schema_registry()
        .oneshot(request.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body)
}

fn base64_basic(user: &str, password: &str) -> String {
    use base64::prelude::*;
    BASE64_STANDARD.encode(format!("{user}:{password}"))
}

fn server() -> KafkaServer {
    KafkaServer::new(KafkaServerConfig {
        advertised_host: "localhost".into(),
        advertised_port: 9092,
        cluster_id: "test".into(),
    })
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // too slow
async fn test_registry_serves_topic_schemas() {
    let server = server();
    let _events = register(
        &server,
        "cdc.events",
        None,
        TopicSchemas {
            key: Some(r#""string""#.into()),
            value: Some(SCHEMA.into()),
        },
    );
    let _untyped = register(&server, "untyped", None, TopicSchemas::default());

    let (status, subjects) = request(&server, "GET", "/subjects", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(subjects, json!(["cdc.events-key", "cdc.events-value"]));

    let (status, versions) = request(
        &server,
        "GET",
        "/subjects/cdc.events-value/versions",
        None,
        None,
    )
    .await;
    assert_eq!((status, versions), (StatusCode::OK, json!([1])));

    let (status, latest) = request(
        &server,
        "GET",
        "/subjects/cdc.events-value/versions/latest",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(latest["subject"], "cdc.events-value");
    assert_eq!(latest["version"], 1);
    assert_eq!(latest["schema"], SCHEMA);
    let id = latest["id"].as_i64().unwrap();

    let (status, by_id) = request(&server, "GET", &format!("/schemas/ids/{id}"), None, None).await;
    assert_eq!((status, by_id), (StatusCode::OK, json!({"schema": SCHEMA})));
    let (status, by_id) = request(
        &server,
        "GET",
        &format!("/schemas/ids/{id}/versions"),
        None,
        None,
    )
    .await;
    assert_eq!(
        (status, by_id),
        (
            StatusCode::OK,
            json!([{"subject": "cdc.events-value", "version": 1}])
        )
    );

    // A schema with the same canonical form registers, and looks up, as the subject's schema.
    let equivalent = r#"{"name": "row", "type": "record", "doc": "a row", "fields": [{"type": "long", "name": "id"}, {"name": "item", "type": "string", "default": ""}]}"#;
    let (status, registered) = request(
        &server,
        "POST",
        "/subjects/cdc.events-value/versions",
        None,
        Some(json!({"schema": equivalent})),
    )
    .await;
    assert_eq!((status, registered), (StatusCode::OK, json!({"id": id})));
    let (status, found) = request(
        &server,
        "POST",
        "/subjects/cdc.events-value",
        None,
        Some(json!({"schema": equivalent, "schemaType": "AVRO"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(found["id"].as_i64(), Some(id));

    // Any other schema is refused.
    let different =
        r#"{"type": "record", "name": "row", "fields": [{"name": "id", "type": "int"}]}"#;
    let (status, error) = request(
        &server,
        "POST",
        "/subjects/cdc.events-value/versions",
        None,
        Some(json!({"schema": different})),
    )
    .await;
    assert_eq!(
        (status, &error["error_code"]),
        (StatusCode::CONFLICT, &json!(409))
    );
    let (status, error) = request(
        &server,
        "POST",
        "/subjects/cdc.events-value",
        None,
        Some(json!({"schema": different})),
    )
    .await;
    assert_eq!(
        (status, &error["error_code"]),
        (StatusCode::NOT_FOUND, &json!(40403))
    );
    let (status, error) = request(
        &server,
        "POST",
        "/subjects/cdc.events-value/versions",
        None,
        Some(json!({"schema": "{not a schema"})),
    )
    .await;
    assert_eq!(
        (status, &error["error_code"]),
        (StatusCode::UNPROCESSABLE_ENTITY, &json!(42201))
    );

    let (status, error) = request(
        &server,
        "GET",
        "/subjects/untyped-value/versions/1",
        None,
        None,
    )
    .await;
    assert_eq!(
        (status, &error["error_code"]),
        (StatusCode::NOT_FOUND, &json!(40401))
    );
    let (status, error) = request(
        &server,
        "GET",
        "/subjects/cdc.events-value/versions/2",
        None,
        None,
    )
    .await;
    assert_eq!(
        (status, &error["error_code"]),
        (StatusCode::NOT_FOUND, &json!(40402))
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // too slow
async fn test_registry_requires_the_topics_credentials() {
    let server = server();
    let credentials = SaslPlainCredentials {
        username: "alice".into(),
        password: "secret".into(),
    };
    let _private = register(
        &server,
        "private",
        Some(credentials),
        TopicSchemas {
            key: None,
            value: Some(SCHEMA.into()),
        },
    );
    let public = register(
        &server,
        "public",
        None,
        TopicSchemas {
            key: None,
            value: Some(SCHEMA.into()),
        },
    );

    let (_, subjects) = request(&server, "GET", "/subjects", None, None).await;
    assert_eq!(subjects, json!(["public-value"]));
    let (_, subjects) = request(&server, "GET", "/subjects", Some(("alice", "secret")), None).await;
    assert_eq!(subjects, json!(["private-value", "public-value"]));
    let (status, error) =
        request(&server, "GET", "/subjects", Some(("alice", "wrong")), None).await;
    assert_eq!(
        (status, &error["error_code"]),
        (StatusCode::UNAUTHORIZED, &json!(40101))
    );
    let (status, _) = request(
        &server,
        "GET",
        "/subjects/private-value/versions/latest",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // A released topic's subjects are gone.
    drop(public);
    let (_, subjects) = request(&server, "GET", "/subjects", None, None).await;
    assert_eq!(subjects, json!([]));
}
