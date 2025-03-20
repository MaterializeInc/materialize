// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::net::Ipv4Addr;
use std::sync::LazyLock;

use hyper::{service, Response, StatusCode};
use hyper_util::rt::TokioIo;
use mz_ccsr::tls::Identity;
use mz_ccsr::{
    Client, CompatibilityLevel, DeleteError, GetByIdError, GetBySubjectError,
    GetSubjectConfigError, PublishError, SchemaReference, SchemaType,
};
use tokio::net::TcpListener;

pub static SCHEMA_REGISTRY_URL: LazyLock<reqwest::Url> =
    LazyLock::new(|| match env::var("SCHEMA_REGISTRY_URL") {
        Ok(addr) => addr.parse().expect("unable to parse SCHEMA_REGISTRY_URL"),
        _ => "http://localhost:8081".parse().unwrap(),
    });

#[mz_ore::test(tokio::test)]
#[cfg_attr(coverage, ignore)] // https://github.com/MaterializeInc/database-issues/issues/5588
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_method` on OS `linux`
#[ignore] // TODO: Reenable when database-issues#8701 is fixed
async fn test_client() -> Result<(), anyhow::Error> {
    let client = mz_ccsr::ClientConfig::new(SCHEMA_REGISTRY_URL.clone()).build()?;

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

    let test_subject = "ccsr-test-schema";

    let schema_v1_id = client
        .publish_schema(test_subject, schema_v1, SchemaType::Avro, &[])
        .await?;
    assert!(schema_v1_id > 0);

    match client
        .publish_schema(test_subject, schema_v2_incompat, SchemaType::Avro, &[])
        .await
    {
        Err(PublishError::IncompatibleSchema) => (),
        res => panic!("expected IncompatibleSchema error, got {:?}", res),
    }

    {
        let res = client.get_schema_by_subject(test_subject).await?;
        assert_eq!(schema_v1_id, res.id);
        assert_raw_schemas_eq(schema_v1, &res.raw);
    }

    let schema_v2_id = client
        .publish_schema(test_subject, schema_v2, SchemaType::Avro, &[])
        .await?;
    assert!(schema_v2_id > 0);
    assert!(schema_v2_id > schema_v1_id);

    assert_eq!(
        schema_v1_id,
        client
            .publish_schema(test_subject, schema_v1, SchemaType::Avro, &[])
            .await?
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
        let res = client.get_schema_by_subject(test_subject).await?;
        assert_eq!(schema_v2_id, res.id);
        assert_raw_schemas_eq(schema_v2, &res.raw);
    }

    assert_eq!(count_schemas(&client, "ccsr-test-").await?, 1);

    client
        .publish_schema("ccsr-test-another-schema", "\"int\"", SchemaType::Avro, &[])
        .await?;
    assert_eq!(count_schemas(&client, "ccsr-test-").await?, 2);

    {
        let subject_with_slashes = "ccsr/test/schema";
        let schema_test_id = client
            .publish_schema(subject_with_slashes, schema_v1, SchemaType::Avro, &[])
            .await?;
        assert!(schema_test_id > 0);

        let res = client.get_schema_by_subject(subject_with_slashes).await?;
        assert_eq!(schema_test_id, res.id);
        assert_raw_schemas_eq(schema_v1, &res.raw);

        let res = client.get_subject_latest(subject_with_slashes).await?;
        assert_eq!(1, res.version);
        assert_eq!(subject_with_slashes, res.name);
        assert_eq!(schema_test_id, res.schema.id);
        assert_raw_schemas_eq(schema_v1, &res.schema.raw);
    }

    // Validate that we can retrieve and change the compatibilty level for a subject
    let initial_res = client.get_subject_config(test_subject).await;
    assert!(matches!(
        initial_res,
        Err(GetSubjectConfigError::SubjectCompatibilityLevelNotSet)
    ));
    client
        .set_subject_compatibility_level(test_subject, CompatibilityLevel::Full)
        .await?;
    let new_compatibility = client
        .get_subject_config(test_subject)
        .await?
        .compatibility_level;
    assert_eq!(new_compatibility, CompatibilityLevel::Full);

    Ok(())
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_method` on OS `linux`
async fn test_client_subject_and_references() -> Result<(), anyhow::Error> {
    let client = mz_ccsr::ClientConfig::new(SCHEMA_REGISTRY_URL.clone()).build()?;

    let existing_subjects = client.list_subjects().await?;
    for s in existing_subjects {
        if s.starts_with("ccsr-test-") {
            client.delete_subject(&s).await?;
        }
    }
    assert_eq!(count_schemas(&client, "ccsr-test-").await?, 0);

    let schema0_subject = "schema0.proto".to_owned();
    let schema0 = r#"
        syntax = "proto3";

        message Choice {
            string field0 = 1;
            int64 field2 = 2;
        }
    "#;

    let schema1_subject = "schema1.proto".to_owned();
    let schema1 = r#"
        syntax = "proto3";
        import "schema0.proto";

        message ChoiceId {
            string id = 1;
            Choice choice = 2;
        }
    "#;

    let schema2_subject = "schema2.proto".to_owned();
    let schema2 = r#"
        syntax = "proto3";

        import "schema0.proto";
        import "schema1.proto";

        message OtherThingWhoKnowWhatEven {
            string whatever = 1;
            ChoiceId nonsense = 2;
            Choice third_field = 3;
        }
    "#;

    let schema0_id = client
        .publish_schema(&schema0_subject, schema0, SchemaType::Protobuf, &[])
        .await?;
    assert!(schema0_id > 0);

    let schema1_id = client
        .publish_schema(
            &schema1_subject,
            schema1,
            SchemaType::Protobuf,
            &[SchemaReference {
                name: schema0_subject.clone(),
                subject: schema0_subject.clone(),
                version: 1,
            }],
        )
        .await?;
    assert!(schema1_id > 0);

    let schema2_id = client
        .publish_schema(
            &schema2_subject,
            schema2,
            SchemaType::Protobuf,
            &[
                SchemaReference {
                    name: schema1_subject.clone(),
                    subject: schema1_subject.clone(),
                    version: 1,
                },
                SchemaReference {
                    name: schema0_subject.clone(),
                    subject: schema0_subject.clone(),
                    version: 1,
                },
            ],
        )
        .await?;
    assert!(schema2_id > 0);

    let (primary_subject, dependency_subjects) =
        client.get_subject_and_references(&schema2_subject).await?;
    assert_eq!(schema2_subject, primary_subject.name);
    assert_eq!(2, dependency_subjects.len());
    assert_eq!(schema0_subject, dependency_subjects[0].name);
    assert_eq!(schema1_subject, dependency_subjects[1].name);

    // Also do the by-id lookup
    let (primary_subject, dependency_subjects) =
        client.get_subject_and_references_by_id(schema2_id).await?;
    assert_eq!(schema2_subject, primary_subject.name);
    assert_eq!(2, dependency_subjects.len());
    assert_eq!(schema0_subject, dependency_subjects[0].name);
    assert_eq!(schema1_subject, dependency_subjects[1].name);

    Ok(())
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_method` on OS `linux`
#[ignore] // TODO: Reenable when database-issues#6818 is fixed
async fn test_client_errors() -> Result<(), anyhow::Error> {
    let invalid_schema_registry_url: reqwest::Url = "data::text/plain,Info".parse().unwrap();
    match mz_ccsr::ClientConfig::new(invalid_schema_registry_url).build() {
        Err(e) => assert_eq!(
            "cannot construct a CCSR client with a cannot-be-a-base URL",
            e.to_string(),
        ),
        res => panic!("Expected error, got {:?}", res),
    }

    let client = mz_ccsr::ClientConfig::new(SCHEMA_REGISTRY_URL.clone()).build()?;

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
    match client
        .publish_schema("ccsr-test-schema", "blah", SchemaType::Avro, &[])
        .await
    {
        Err(PublishError::InvalidSchema { .. }) => (),
        res => panic!("expected PublishError::InvalidSchema, got {:?}", res),
    }

    // Delete-specific errors.
    match client.delete_subject("ccsr-test-noexist").await {
        Err(DeleteError::SubjectNotFound) => (),
        res => panic!("expected DeleteError::SubjectNotFound, got {:?}", res),
    }

    Ok(())
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_method` on OS `linux`
async fn test_server_errors() -> Result<(), anyhow::Error> {
    // When the schema registry gracefully reports an error by including a
    // properly-formatted JSON document in the response, the specific error code
    // and message should be propagated.

    let client_graceful = start_server(
        StatusCode::INTERNAL_SERVER_ERROR,
        r#"{ "error_code": 50001, "message": "overloaded; try again later" }"#,
    )
    .await?;

    match client_graceful
        .publish_schema("foo", "bar", SchemaType::Avro, &[])
        .await
    {
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
    )
    .await?;

    match client_crash
        .publish_schema("foo", "bar", SchemaType::Avro, &[])
        .await
    {
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

async fn start_server(
    status_code: StatusCode,
    body: &'static str,
) -> Result<Client, anyhow::Error> {
    let addr = (Ipv4Addr::LOCALHOST, 0);
    let listener = TcpListener::bind(addr).await.unwrap();
    let addr = listener.local_addr().unwrap();

    mz_ore::task::spawn(|| "start_server", async move {
        loop {
            let (conn, remote_addr) = listener.accept().await.unwrap();
            mz_ore::task::spawn(|| format!("start_server:{remote_addr}"), async move {
                let service = service::service_fn(move |_req| async move {
                    Response::builder()
                        .status(status_code)
                        .body(body.to_string())
                });
                if let Err(error) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(conn), service)
                    .await
                {
                    eprintln!("error handling client connection: {error}");
                }
            });
        }
    });

    let url: reqwest::Url = format!("http://{}", addr).parse().unwrap();
    mz_ccsr::ClientConfig::new(url).build()
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

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_method` on OS `linux`
fn test_stack_from_pem_error() {
    // Note that this file has file has a malformed certificate after the end of
    // the private key. This is also not a private key in use anywhere to our
    // knowledge.
    let certs = r#"-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDDC5MP3v1BHOgI
5SsmrW8mjxzQGOz0IlC5jp1muW/kpEoE9TG317TEnO5Uye6zZudkFCP8YGEiN3Mc
FbTM7eX6PjAPdnGU7khuUt/20ZM+NX5kWZPrmPTh4WQaDCL7ah1LqzBaUAMaSXq8
iuy7LGJNF8wdx8L5BjDiGTTxZXOg0Haxknc7Mbiwc9z8eb7omvzQzsOwyqocrF2u
z86TzX1jtHP48i5CxoRHKxE94De3tNxjT/Y3OZlS4QS7iekAOQ04DVV3GIHvRUXN
2H8ayy4+yOdhHn6ER5Jn3lti1Q5XSrxkrYn7L1Vcj6IwZQhhF5vc+ovxOYb+8ert
Eo97tIkLAgMBAAECggEAQteHHRPKz9Mzs8Sxvo4GPv0hnzFDl0DhUE4PJCKdtYoV
8dADq2DJiu3LAZS4cJPt7Y63bGitMRg2oyPPM8G9pD5Goy3wq9zjRqexKDlXUCTt
/T7zofRny7c94m1RWb7ablGq/vBXt90BqnajvVtvDsN+iKAqccQM4ZdI3QdrEmt1
cHex924itzG/mqbFTAfAmVj1ZsRnJp55Txy2gqq7jX00xDM8+H49SRvUu49N64LQ
6BUWCgWCJePRtgjSHjboAzPqSkMdaTE/WDY2zgGF3Qfq4f6JCHKfm4QylCH4gYUU
1Kf7ttmhu9NoZO+hczobKkxP9RtXfyTRH2bsJXy2HQKBgQDhHgavxk/ln5mdMGGw
rQud2vF9n7UwFiysYxocIC5/CWD0GAhnawchjPypbW/7vKM5Z9zhW3eH1U9P13sa
2xHfrU5BZ16rxoBbKNpcr7VeEbUBAsDoGV24xjoecp7rB2hZ+mGik5/5Ig1Rk1KH
dcvYy2KSi1h4Sm+mXwimmA4VDQKBgQDdzW+5FPbdM2sUB2gLMQtn3ICjDSu6IQ+k
d0p3WlTIT51RUsPXXKkk96O5anUbeB3syY8tSKPGggsaXaeL3o09yIamtERgCnn3
d9IS+4VKPWQlFUICU1KrD+TO7IYIX04iXBuVE5ihv0q3mslhDotmX4kS38NtKEFF
jLjA2RvAdwKBgAFkIxxw+Ett+hALnX7vAtRd5wIku4TpjisejanA1Si50RyRDXQ+
KBQf/+u4HmoK12Nibe4Cl7GCMvRGW59l3S1pr8MdtWsQVfi6Puc1usQzDdBMyQ5m
IbsjlnZbtPm02QM9Vd8gVGvAtx5a77aglrrnPtuy+r/7jccUbURCSkv9AoGAH9m3
WGmVRZBzqO2jWDATxjdY1ZE3nUPQHjrvG5KCKD2ehqYO72cj9uYEwcRyyp4GFhGf
mM4cjo3wEDowrBoqSBv6kgfC5dO7TfkL1qP9sPp93gFeeD0E2wGuRrSaTqt46eA2
KcMloNx6W0FD98cB55KCeY5eXtdwAA/EHBVRMeMCgYAd3n6PcL6rVXyE3+wRTKK4
+zvx5sjTAnljr5ttbEnpZafzrYIfDpB8NNjexy83AeC0O13LvSHIFoTwP8sywJRO
RxbPMjhEBdVZ5NxlxYer7yKN+h5OBJfrLswPku7y4vdFYK3x/lMuNQO61hb1VFHc
T2BDTbF0QSlPxFsv18B9zg==
-----END PRIVATE KEY-----
x"#;
    Identity::from_pem(certs.as_bytes(), &[]).unwrap_err();
}
