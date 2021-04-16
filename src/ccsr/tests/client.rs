// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;

use hyper::server::conn::AddrIncoming;
use hyper::service;
use hyper::Server;
use hyper::StatusCode;
use hyper::{Body, Response};
use lazy_static::lazy_static;

use ccsr::{Client, DeleteError, GetByIdError, GetBySubjectError, PublishError};

lazy_static! {
    pub static ref SCHEMA_REGISTRY_URL: reqwest::Url = match env::var("SCHEMA_REGISTRY_URL") {
        Ok(addr) => addr.parse().expect("unable to parse SCHEMA_REGISTRY_URL"),
        _ => "http://localhost:8081".parse().unwrap(),
    };
}

#[tokio::test]
async fn test_client() -> Result<(), anyhow::Error> {
    let client = ccsr::ClientConfig::new(SCHEMA_REGISTRY_URL.clone()).build()?;

    let existing_subjects = client.list_subjects().await?;
    for s in existing_subjects {
        if s.starts_with("ccsr-test-") {
            client.delete_subject(&s).await?;
        }
    }

    let schema_v1 = r#"{ "type": "record", "name": "na", "fields": [
        { "name": "a", "type": "long" }
    ]}"#;

    let schema_v2 = r#"{ "type": "record", "name": "na", "fields": [
        { "name": "a", "type": "long" },
        { "name": "b", "type": "long", "default": 0 }
    ]}"#;

    let schema_v2_incompat = r#"{ "type": "record", "name": "na", "fields": [
        { "name": "a", "type": "string" }
    ]}"#;

    assert_eq!(count_schemas(&client, "ccsr-test-").await?, 0);

    let schema_v1_id = client.publish_schema("ccsr-test-schema", schema_v1).await?;
    assert!(schema_v1_id > 0);

    match client
        .publish_schema("ccsr-test-schema", schema_v2_incompat)
        .await
    {
        Err(PublishError::IncompatibleSchema) => (),
        res => panic!("expected IncompatibleSchema error, got {:?}", res),
    }

    {
        let res = client.get_schema_by_subject("ccsr-test-schema").await?;
        assert_eq!(schema_v1_id, res.id);
        assert_raw_schemas_eq(schema_v1, &res.raw);
    }

    let schema_v2_id = client.publish_schema("ccsr-test-schema", schema_v2).await?;
    assert!(schema_v2_id > 0);
    assert!(schema_v2_id > schema_v1_id);

    assert_eq!(
        schema_v1_id,
        client.publish_schema("ccsr-test-schema", schema_v1).await?
    );

    {
        let res1 = client.get_schema_by_id(schema_v1_id).await?;
        let res2 = client.get_schema_by_id(schema_v2_id).await?;
        assert_eq!(schema_v1_id, res1.id);
        assert_eq!(schema_v2_id, res2.id);
        assert_raw_schemas_eq(schema_v1, &res1.raw);
        assert_raw_schemas_eq(schema_v2, &res2.raw);
    }

    {
        let res = client.get_schema_by_subject("ccsr-test-schema").await?;
        assert_eq!(schema_v2_id, res.id);
        assert_raw_schemas_eq(schema_v2, &res.raw);
    }

    assert_eq!(count_schemas(&client, "ccsr-test-").await?, 1);

    client
        .publish_schema("ccsr-test-another-schema", "\"int\"")
        .await?;
    assert_eq!(count_schemas(&client, "ccsr-test-").await?, 2);

    Ok(())
}

#[tokio::test]
async fn test_client_errors() -> Result<(), anyhow::Error> {
    let client = ccsr::ClientConfig::new(SCHEMA_REGISTRY_URL.clone()).build()?;

    // Get-by-id-specific errors.
    match client.get_schema_by_id(i32::max_value()).await {
        Err(GetByIdError::SchemaNotFound) => (),
        res => panic!("expected GetError::SchemaNotFound, got {:?}", res),
    }

    // Get-by-subject-specific errors.
    match client.get_schema_by_subject("ccsr-test-noexist").await {
        Err(GetBySubjectError::SubjectNotFound) => (),
        res => panic!("expected GetBySubjectError::SubjectNotFound, got {:?}", res),
    }

    // Publish-specific errors.
    match client.publish_schema("ccsr-test-schema", "blah").await {
        Err(PublishError::InvalidSchema) => (),
        res => panic!("expected PublishError::InvalidSchema, got {:?}", res),
    }

    // Delete-specific errors.
    match client.delete_subject("ccsr-test-noexist").await {
        Err(DeleteError::SubjectNotFound) => (),
        res => panic!("expected DeleteError::SubjectNotFound, got {:?}", res),
    }

    Ok(())
}

#[tokio::test]
async fn test_server_errors() -> Result<(), anyhow::Error> {
    // When the schema registry gracefully reports an error by including a
    // properly-formatted JSON document in the response, the specific error code
    // and message should be propagated.

    let client_graceful = start_server(
        StatusCode::INTERNAL_SERVER_ERROR,
        r#"{ "error_code": 50001, "message": "overloaded; try again later" }"#,
    )?;

    match client_graceful.publish_schema("foo", "bar").await {
        Err(PublishError::Server {
            code: 50001,
            ref message,
        }) if message == "overloaded; try again later" => (),
        res => panic!("expected PublishError::Server, got {:?}", res),
    }

    match client_graceful.get_schema_by_id(0).await {
        Err(GetByIdError::Server {
            code: 50001,
            ref message,
        }) if message == "overloaded; try again later" => (),
        res => panic!("expected GetByIdError::Server, got {:?}", res),
    }

    match client_graceful.get_schema_by_subject("foo").await {
        Err(GetBySubjectError::Server {
            code: 50001,
            ref message,
        }) if message == "overloaded; try again later" => (),
        res => panic!("expected GetBySubjectError::Server, got {:?}", res),
    }

    match client_graceful.delete_subject("foo").await {
        Err(DeleteError::Server {
            code: 50001,
            ref message,
        }) if message == "overloaded; try again later" => (),
        res => panic!("expected DeleteError::Server, got {:?}", res),
    }

    // If the schema registry crashes so hard that it spits out an exception
    // handler in the response, we should report the HTTP status code and a
    // generic message indicating that no further details were available.
    let client_crash = start_server(
        StatusCode::INTERNAL_SERVER_ERROR,
        r#"panic! an exception occured!"#,
    )?;

    match client_crash.publish_schema("foo", "bar").await {
        Err(PublishError::Server {
            code: 500,
            ref message,
        }) if message == "unable to decode error details" => (),
        res => panic!("expected PublishError::Server, got {:?}", res),
    }

    match client_crash.get_schema_by_id(0).await {
        Err(GetByIdError::Server {
            code: 500,
            ref message,
        }) if message == "unable to decode error details" => (),
        res => panic!("expected GetError::Server, got {:?}", res),
    }

    match client_crash.get_schema_by_subject("foo").await {
        Err(GetBySubjectError::Server {
            code: 500,
            ref message,
        }) if message == "unable to decode error details" => (),
        res => panic!("expected GetError::Server, got {:?}", res),
    }

    match client_crash.delete_subject("foo").await {
        Err(DeleteError::Server {
            code: 500,
            ref message,
        }) if message == "unable to decode error details" => (),
        res => panic!("expected DeleteError::Server, got {:?}", res),
    }

    Ok(())
}

fn start_server(status_code: StatusCode, body: &'static str) -> Result<Client, anyhow::Error> {
    let addr = {
        let incoming = AddrIncoming::bind(&([127, 0, 0, 1], 0).into()).unwrap();
        let addr = incoming.local_addr();
        let server =
            Server::builder(incoming).serve(service::make_service_fn(move |_conn| async move {
                Ok::<_, hyper::Error>(service::service_fn(move |_req| async move {
                    Response::builder()
                        .status(status_code)
                        .body(Body::from(body))
                }))
            }));
        tokio::spawn(async {
            match server.await {
                Ok(()) => (),
                Err(err) => eprintln!("server error: {}", err),
            }
        });
        addr
    };

    let url: reqwest::Url = format!("http://{}", addr).parse().unwrap();
    ccsr::ClientConfig::new(url).build()
}

fn assert_raw_schemas_eq(schema1: &str, schema2: &str) {
    let schema1: serde_json::Value = serde_json::from_str(schema1).unwrap();
    let schema2: serde_json::Value = serde_json::from_str(schema2).unwrap();
    assert_eq!(schema1, schema2);
}

async fn count_schemas(client: &Client, subject_prefix: &str) -> Result<usize, anyhow::Error> {
    Ok(client
        .list_subjects()
        .await?
        .iter()
        .filter(|s| s.starts_with(subject_prefix))
        .count())
}
