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

use hyper::{Response, StatusCode, service};
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
    match client.get_schema_by_id(i32::MAX).await {
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
fn test_invalid_tls_config_returns_error() {
    use mz_ccsr::tls::Certificate;

    // Verify that invalid TLS material is caught at construction time, not at
    // ClientConfig::build() time (where it previously caused a panic via .unwrap()).
    let err = Identity::from_pem(b"not a key", b"not a certificate");
    assert!(err.is_err(), "garbage PEM identity should be rejected");

    let err = Certificate::from_pem(b"not a certificate");
    assert!(err.is_err(), "garbage PEM cert should be rejected");

    let err = Certificate::from_der(&[0xDE, 0xAD]);
    assert!(err.is_err(), "garbage DER cert should be rejected");
}

/// A test certificate and its private key.
struct TestCert {
    key: openssl::pkey::PKey<openssl::pkey::Private>,
    cert: openssl::x509::X509,
}

impl TestCert {
    /// Generates a certificate with common name `cn`, a DNS subjectAltName
    /// `san` if given, and `CA:TRUE` if `ca`. It is signed by `issuer`, or
    /// self-signed if `issuer` is `None`. It is valid from now until a day
    /// from now.
    fn new(cn: &str, san: Option<&str>, ca: bool, issuer: Option<&TestCert>) -> TestCert {
        Self::with_validity(cn, san, ca, issuer, 0, 1)
    }

    /// Like [`TestCert::new`], but valid from `not_before` to `not_after`
    /// days from now.
    fn with_validity(
        cn: &str,
        san: Option<&str>,
        ca: bool,
        issuer: Option<&TestCert>,
        not_before: i64,
        not_after: i64,
    ) -> TestCert {
        Self::generate(&[cn], san, None, ca, issuer, not_before, not_after)
    }

    /// Generates a self-signed `CA:TRUE` certificate with the common names
    /// `cns`, in order, and the subjectAltNames `dns_san` and `ip_san`, if
    /// given.
    fn pinned(cns: &[&str], dns_san: Option<&str>, ip_san: Option<&str>) -> TestCert {
        Self::generate(cns, dns_san, ip_san, true, None, 0, 1)
    }

    fn generate(
        cns: &[&str],
        dns_san: Option<&str>,
        ip_san: Option<&str>,
        ca: bool,
        issuer: Option<&TestCert>,
        not_before: i64,
        not_after: i64,
    ) -> TestCert {
        use openssl::asn1::Asn1Time;
        use openssl::bn::{BigNum, MsbOption};
        use openssl::ec::{EcGroup, EcKey};
        use openssl::hash::MessageDigest;
        use openssl::nid::Nid;
        use openssl::pkey::PKey;
        use openssl::x509::extension::{
            BasicConstraints, ExtendedKeyUsage, SubjectAlternativeName,
        };
        use openssl::x509::{X509, X509NameBuilder};

        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
        let key = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        for cn in cns {
            name.append_entry_by_text("CN", cn).unwrap();
        }
        let name = name.build();
        let mut serial = BigNum::new().unwrap();
        serial.rand(64, MsbOption::MAYBE_ZERO, false).unwrap();

        let mut cert = X509::builder().unwrap();
        cert.set_version(2).unwrap();
        cert.set_serial_number(&serial.to_asn1_integer().unwrap())
            .unwrap();
        cert.set_subject_name(&name).unwrap();
        cert.set_issuer_name(issuer.map_or(&name, |i| i.cert.subject_name()))
            .unwrap();
        cert.set_pubkey(&key).unwrap();
        let days_from_now = |days: i64| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            Asn1Time::from_unix(i64::try_from(now).unwrap() + days * 86_400).unwrap()
        };
        cert.set_not_before(&days_from_now(not_before)).unwrap();
        cert.set_not_after(&days_from_now(not_after)).unwrap();
        let mut constraints = BasicConstraints::new();
        constraints.critical();
        if ca {
            constraints.ca();
        }
        cert.append_extension(constraints.build().unwrap()).unwrap();
        if !ca {
            let eku = ExtendedKeyUsage::new()
                .server_auth()
                .client_auth()
                .build()
                .unwrap();
            cert.append_extension(eku).unwrap();
        }
        if dns_san.is_some() || ip_san.is_some() {
            let mut san = SubjectAlternativeName::new();
            if let Some(dns) = dns_san {
                san.dns(dns);
            }
            if let Some(ip) = ip_san {
                san.ip(ip);
            }
            let ctx = cert.x509v3_context(issuer.map(|i| &*i.cert), None);
            cert.append_extension(san.build(&ctx).unwrap()).unwrap();
        }
        let signer = issuer.map_or(&key, |i| &i.key);
        cert.sign(signer, MessageDigest::sha256()).unwrap();
        TestCert {
            key,
            cert: cert.build(),
        }
    }

    fn cert_pem(&self) -> Vec<u8> {
        self.cert.to_pem().unwrap()
    }
}

/// Returns a PEM-encoded PKCS #8 key and a self-signed certificate for it.
fn self_signed_pem() -> (Vec<u8>, Vec<u8>) {
    let cert = TestCert::new("ccsr-test", None, false, None);
    (
        cert.key.private_key_to_pem_pkcs8().unwrap(),
        cert.cert_pem(),
    )
}

const TLS_TEST_HOST: &str = "sr.test";

/// Starts an HTTPS server presenting `server` and returns a client for it that
/// trusts `root`.
async fn start_tls_server(server: &TestCert, root: &TestCert) -> Client {
    let addr = start_tls_listener(server, None).await;
    tls_client_config(addr, root).build().unwrap()
}

/// Returns a client configuration for a server started by
/// [`start_tls_listener`] that trusts `root`.
fn tls_client_config(addr: std::net::SocketAddr, root: &TestCert) -> mz_ccsr::ClientConfig {
    let url = format!("https://{TLS_TEST_HOST}:{}", addr.port())
        .parse()
        .unwrap();
    let root = mz_ccsr::tls::Certificate::from_pem(&root.cert_pem()).unwrap();
    mz_ccsr::ClientConfig::new(url)
        .resolve_to_addrs(TLS_TEST_HOST, &[addr])
        .add_root_certificate(root)
}

/// Starts an HTTPS server presenting `server`. If `client_ca` is given, the
/// server requires a client certificate signed by it.
async fn start_tls_listener(
    server: &TestCert,
    client_ca: Option<&TestCert>,
) -> std::net::SocketAddr {
    use openssl::ssl::{Ssl, SslAcceptor, SslMethod, SslVerifyMode};

    let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
    acceptor.set_private_key(&server.key).unwrap();
    acceptor.set_certificate(&server.cert).unwrap();
    if let Some(client_ca) = client_ca {
        acceptor
            .cert_store_mut()
            .add_cert(client_ca.cert.clone())
            .unwrap();
        acceptor.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
    }
    let acceptor = acceptor.build();

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    mz_ore::task::spawn(|| "start_tls_server", async move {
        loop {
            let (conn, _) = listener.accept().await.unwrap();
            let ssl = Ssl::new(acceptor.context()).unwrap();
            let mut stream = tokio_openssl::SslStream::new(ssl, conn).unwrap();
            mz_ore::task::spawn(|| "start_tls_server:conn", async move {
                // Some tests expect the handshake to fail.
                if std::pin::Pin::new(&mut stream).accept().await.is_err() {
                    return;
                }
                let service = service::service_fn(|_req| async {
                    Response::builder()
                        .status(StatusCode::OK)
                        .body("[]".to_string())
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    addr
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_tls_server_verification() {
    use mz_ore::error::ErrorExt;

    let ca = TestCert::new("ccsr test ca", None, true, None);

    let leaf = TestCert::new(TLS_TEST_HOST, Some(TLS_TEST_HOST), false, Some(&ca));
    let client = start_tls_server(&leaf, &ca).await;
    client
        .list_subjects()
        .await
        .expect("SAN leaf signed by the root should be accepted");

    // webpki rejects a `CA:TRUE` server certificate as `CaUsedAsEndEntity`.
    // It is accepted because it is identical to the configured root.
    let self_signed_ca = TestCert::new(TLS_TEST_HOST, Some(TLS_TEST_HOST), true, None);
    let client = start_tls_server(&self_signed_ca, &self_signed_ca).await;
    client
        .list_subjects()
        .await
        .expect("server certificate identical to the root should be accepted");

    // A pinned certificate without a subjectAltName, as `openssl req -x509
    // -subj /CN=<host>` produces, matches on its common name.
    let cn_only_pinned = TestCert::new(TLS_TEST_HOST, None, true, None);
    let client = start_tls_server(&cn_only_pinned, &cn_only_pinned).await;
    client
        .list_subjects()
        .await
        .expect("pinned certificate whose common name matches should be accepted");

    let cn_only_wrong_name = TestCert::new("other.test", None, true, None);
    let client = start_tls_server(&cn_only_wrong_name, &cn_only_wrong_name).await;
    let err = client.list_subjects().await.unwrap_err();
    let err = err.display_with_causes().to_string();
    assert!(
        err.contains("not valid for name"),
        "unexpected error: {err}"
    );

    // Any common name may match.
    for cns in [[TLS_TEST_HOST, "other.test"], ["other.test", TLS_TEST_HOST]] {
        let pinned = TestCert::pinned(&cns, None, None);
        let client = start_tls_server(&pinned, &pinned).await;
        client
            .list_subjects()
            .await
            .expect("pinned certificate with a matching common name should be accepted");
    }

    // Stricter than OpenSSL: an IP subjectAltName also disables the common
    // name fallback.
    let ip_san = TestCert::pinned(&[TLS_TEST_HOST], None, Some("127.0.0.1"));
    let client = start_tls_server(&ip_san, &ip_san).await;
    let err = client.list_subjects().await.unwrap_err();
    let err = err.display_with_causes().to_string();
    assert!(
        err.contains("not valid for name"),
        "unexpected error: {err}"
    );

    // A subjectAltName takes precedence over a matching common name.
    let wrong_name = TestCert::new(TLS_TEST_HOST, Some("other.test"), true, None);
    let client = start_tls_server(&wrong_name, &wrong_name).await;
    let err = client.list_subjects().await.unwrap_err();
    let err = err.display_with_causes().to_string();
    assert!(
        err.contains("not valid for name"),
        "unexpected error: {err}"
    );

    for (not_before, not_after, expected) in [(-2, -1, "Expired"), (1, 2, "NotValidYet")] {
        let pinned = TestCert::with_validity(
            TLS_TEST_HOST,
            Some(TLS_TEST_HOST),
            true,
            None,
            not_before,
            not_after,
        );
        let client = start_tls_server(&pinned, &pinned).await;
        let err = client.list_subjects().await.unwrap_err();
        let err = err.display_with_causes().to_string();
        assert!(err.contains(expected), "unexpected error: {err}");
    }

    let other = TestCert::new(TLS_TEST_HOST, Some(TLS_TEST_HOST), true, None);
    let client = start_tls_server(&self_signed_ca, &other).await;
    let err = client.list_subjects().await.unwrap_err();
    let err = err.display_with_causes().to_string();
    assert!(
        err.contains("invalid peer certificate"),
        "unexpected error: {err}"
    );

    // webpki does not fall back to the common name when there is no
    // subjectAltName.
    let cn_only = TestCert::new(TLS_TEST_HOST, None, false, Some(&ca));
    let client = start_tls_server(&cn_only, &ca).await;
    let err = client.list_subjects().await.unwrap_err();
    let err = err.display_with_causes().to_string();
    // NOTE: On macOS the platform verifier delegates to Security.framework,
    // which words this error differently.
    let expected = if cfg!(target_os = "linux") {
        "not valid for name"
    } else {
        "invalid peer certificate"
    };
    assert!(err.contains(expected), "unexpected error: {err}");
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_tls_client_identity_with_roots() {
    let ca = TestCert::new("ccsr test ca", None, true, None);
    let server = TestCert::new(TLS_TEST_HOST, Some(TLS_TEST_HOST), false, Some(&ca));
    let client_cert = TestCert::new("ccsr test client", None, false, Some(&ca));
    let addr = start_tls_listener(&server, Some(&ca)).await;

    let identity = Identity::from_pem(
        &client_cert.key.private_key_to_pem_pkcs8().unwrap(),
        &client_cert.cert_pem(),
    )
    .unwrap();
    let client = tls_client_config(addr, &ca)
        .identity(identity)
        .build()
        .unwrap();
    client
        .list_subjects()
        .await
        .expect("client certificate should be presented");

    let client = tls_client_config(addr, &ca).build().unwrap();
    client
        .list_subjects()
        .await
        .expect_err("server requires a client certificate");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
fn test_pem_identity() {
    use mz_ccsr::tls::Certificate;

    let (key, cert) = self_signed_pem();
    let ident = Identity::from_pem(&key, &cert).unwrap();
    assert_eq!(format!("{ident:?}"), "Identity { .. }");
    mz_ccsr::ClientConfig::new(reqwest::Url::parse("https://localhost").unwrap())
        .add_root_certificate(Certificate::from_pem(&cert).unwrap())
        .identity(ident)
        .build()
        .unwrap();

    let (other_key, _) = self_signed_pem();
    let err = Identity::from_pem(&other_key, &cert).unwrap_err();
    assert!(
        err.to_string().contains("KeyMismatch"),
        "unexpected error: {err}"
    );
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
