// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for TLS encryption and authentication.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::future::IntoFuture;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, TcpStream};
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use chrono::Utc;
use headers::Authorization;
use http::header::{COOKIE, SET_COOKIE};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::http::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use hyper::http::uri::Scheme;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_openssl::client::legacy::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use itertools::Itertools;
use jsonwebtoken::{self, DecodingKey, EncodingKey};
use mz_auth::password::Password;
use mz_environmentd::test_util::{self, Ca, make_header, make_pg_tls};
use mz_environmentd::{WebSocketAuth, WebSocketResponse};
use mz_frontegg_auth::{
    Authenticator as FronteggAuthentication, AuthenticatorConfig as FronteggConfig, ClaimMetadata,
    ClaimTokenType, Claims, DEFAULT_REFRESH_DROP_FACTOR, DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
};
use mz_frontegg_mock::{
    FronteggMockServer, models::ApiToken, models::TenantApiTokenConfig, models::UserConfig,
};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{NowFn, SYSTEM_TIME};
use mz_ore::retry::Retry;
use mz_ore::{assert_contains, assert_err, assert_none, assert_ok};
use mz_sql::names::PUBLIC_ROLE_NAME;
use mz_sql::session::user::{HTTP_DEFAULT_USER, SYSTEM_USER};
use openssl::error::ErrorStack;
use openssl::ssl::{SslConnector, SslConnectorBuilder, SslMethod, SslOptions, SslVerifyMode};
use postgres::config::SslMode;
use postgres::error::SqlState;
use serde::Deserialize;
use serde_json::json;
use tokio::time::sleep;
use tungstenite::client::ClientRequestBuilder;
use tungstenite::protocol::CloseFrame;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::{Message, Utf8Bytes};
use uuid::Uuid;

// How long, in seconds, a claim is valid for. Increasing this value will decrease some test flakes
// without increasing test time.
const EXPIRES_IN_SECS: u64 = 20;

fn make_http_tls<F>(configure: F) -> HttpsConnector<HttpConnector>
where
    F: Fn(&mut SslConnectorBuilder) -> Result<(), ErrorStack>,
{
    let mut connector_builder = SslConnector::builder(SslMethod::tls()).unwrap();
    // See comment in `make_pg_tls` about disabling TLS v1.3.
    let options = connector_builder.options() | SslOptions::NO_TLSV1_3;
    connector_builder.set_options(options);
    configure(&mut connector_builder).unwrap();
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    HttpsConnector::with_connector(http, connector_builder).unwrap()
}

fn make_ws_tls<F>(uri: &Uri, configure: F) -> impl Read + Write + use<F>
where
    F: Fn(&mut SslConnectorBuilder) -> Result<(), ErrorStack>,
{
    let mut connector_builder = SslConnector::builder(SslMethod::tls()).unwrap();
    // See comment in `make_pg_tls` about disabling TLS v1.3.
    let options = connector_builder.options() | SslOptions::NO_TLSV1_3;
    connector_builder.set_options(options);
    configure(&mut connector_builder).unwrap();
    let connector = connector_builder.build();

    let stream =
        TcpStream::connect(format!("{}:{}", uri.host().unwrap(), uri.port().unwrap())).unwrap();
    connector.connect(uri.host().unwrap(), stream).unwrap()
}

// Use two error types because some tests need to retry certain errors because
// there's a race condition for which is produced and they always need a
// postgres-style error.
enum Assert<E, D = ()> {
    Success,
    SuccessSuperuserCheck(bool),
    Err(E),
    DbErr(D),
}

enum TestCase<'a> {
    Pgwire {
        user_to_auth_as: &'a str,
        user_reported_by_system: &'a str,
        password: Option<Cow<'a, str>>,
        ssl_mode: SslMode,
        configure: Box<dyn Fn(&mut SslConnectorBuilder) -> Result<(), ErrorStack> + 'a>,
        assert: Assert<
            // A non-retrying, raw error.
            Box<dyn Fn(&tokio_postgres::error::Error) + 'a>,
            // A check that retries until it gets a DbError.
            Box<dyn Fn(&tokio_postgres::error::DbError) + 'a>,
        >,
    },
    Http {
        user_to_auth_as: &'a str,
        user_reported_by_system: &'a str,
        scheme: Scheme,
        headers: &'a HeaderMap,
        configure: Box<dyn Fn(&mut SslConnectorBuilder) -> Result<(), ErrorStack> + 'a>,
        assert: Assert<Box<dyn Fn(Option<StatusCode>, String) + 'a>>,
    },
    Ws {
        auth: &'a WebSocketAuth,
        configure: Box<dyn Fn(&mut SslConnectorBuilder) -> Result<(), ErrorStack> + 'a>,
        assert: Assert<Box<dyn Fn(CloseCode, String) + 'a>>,
    },
}

fn assert_http_rejected() -> Assert<Box<dyn Fn(Option<StatusCode>, String)>> {
    Assert::Err(Box::new(|code, message| {
        const ALLOWED_MESSAGES: [&str; 3] = [
            "Connection reset by peer",
            "connection closed before message completed",
            "invalid HTTP version parsed",
        ];
        assert_eq!(code, None);
        if !ALLOWED_MESSAGES
            .iter()
            .any(|allowed| message.contains(allowed))
        {
            panic!("TLS rejected with unexpected error message: {}", message)
        }
    }))
}

async fn run_tests<'a>(header: &str, server: &test_util::TestServer, tests: &[TestCase<'a>]) {
    println!("==> {}", header);
    for test in tests {
        match test {
            TestCase::Pgwire {
                user_to_auth_as,
                user_reported_by_system,
                password,
                ssl_mode,
                configure,
                assert,
            } => {
                println!(
                    "pgwire user={} password={:?} ssl_mode={:?}",
                    user_to_auth_as, password, ssl_mode
                );

                let tls = make_pg_tls(configure);
                let password = password.as_ref().unwrap_or(&Cow::Borrowed(""));
                let conn_config = server
                    .connect()
                    .ssl_mode(*ssl_mode)
                    .user(user_to_auth_as)
                    .password(password);

                match assert {
                    Assert::Success => {
                        let pg_client = conn_config.with_tls(tls).await.unwrap();
                        let row = pg_client
                            .query_one("SELECT current_user", &[])
                            .await
                            .unwrap();
                        assert_eq!(row.get::<_, String>(0), *user_reported_by_system);
                    }
                    Assert::SuccessSuperuserCheck(is_superuser) => {
                        let pg_client = conn_config.with_tls(tls).await.unwrap();
                        let row = pg_client
                            .query_one("SELECT current_user", &[])
                            .await
                            .unwrap();
                        assert_eq!(row.get::<_, String>(0), *user_reported_by_system);

                        let row = pg_client.query_one("SHOW is_superuser", &[]).await.unwrap();
                        let expected = if *is_superuser { "on" } else { "off" };
                        assert_eq!(row.get::<_, String>(0), *expected);
                    }
                    Assert::DbErr(check) => {
                        // This sometimes returns a network error, so retry until we get a db error.
                        Retry::default()
                            .max_duration(Duration::from_secs(10))
                            .retry_async(|_| async {
                                let Err(err) = server
                                    .connect()
                                    .with_config(conn_config.as_pg_config().clone())
                                    .with_tls(tls.clone())
                                    .await
                                else {
                                    return Err(());
                                };
                                let Some(err) = err.as_db_error() else {
                                    return Err(());
                                };
                                check(err);
                                Ok(())
                            })
                            .await
                            .unwrap();
                    }
                    Assert::Err(check) => {
                        let pg_client = conn_config.with_tls(tls.clone()).await;
                        let err = match pg_client {
                            Ok(_) => panic!("connection unexpectedly succeeded"),
                            Err(err) => err,
                        };
                        check(&err);
                    }
                }
            }
            TestCase::Http {
                user_to_auth_as,
                user_reported_by_system,
                scheme,
                headers,
                configure,
                assert,
            } => {
                async fn query_http_api<'a>(
                    query: &str,
                    uri: &Uri,
                    headers: &'a HeaderMap,
                    configure: &Box<
                        dyn Fn(&mut SslConnectorBuilder) -> Result<(), ErrorStack> + 'a,
                    >,
                ) -> Result<Response<Incoming>, hyper_util::client::legacy::Error> {
                    hyper_util::client::legacy::Client::builder(TokioExecutor::new())
                        .build(make_http_tls(configure))
                        .request({
                            let mut req = Request::post(uri);
                            for (k, v) in headers.iter() {
                                req.headers_mut().unwrap().insert(k, v.clone());
                            }
                            req.headers_mut().unwrap().insert(
                                "Content-Type",
                                HeaderValue::from_static("application/json"),
                            );
                            req.body(json!({ "query": query }).to_string()).unwrap()
                        })
                        .await
                }

                async fn assert_success_response(
                    res: Result<Response<Incoming>, hyper_util::client::legacy::Error>,
                    expected_rows: Vec<Vec<String>>,
                ) {
                    #[derive(Deserialize)]
                    struct Result {
                        rows: Vec<Vec<String>>,
                    }
                    #[derive(Deserialize)]
                    struct Response {
                        results: Vec<Result>,
                    }
                    let body = res.unwrap().into_body().collect().await.unwrap().to_bytes();
                    let res: Response = serde_json::from_slice(&body).unwrap();
                    assert_eq!(res.results[0].rows, expected_rows)
                }

                println!("http user={} scheme={}", user_to_auth_as, scheme);

                let uri = Uri::builder()
                    .scheme(scheme.clone())
                    .authority(&*format!(
                        "{}:{}",
                        Ipv4Addr::LOCALHOST,
                        server.http_local_addr().port()
                    ))
                    .path_and_query("/api/sql")
                    .build()
                    .unwrap();
                let res =
                    query_http_api("SELECT pg_catalog.current_user()", &uri, headers, configure)
                        .await;

                match assert {
                    Assert::Success => {
                        assert_success_response(
                            res,
                            vec![vec![user_reported_by_system.to_string()]],
                        )
                        .await;
                    }
                    Assert::SuccessSuperuserCheck(is_superuser) => {
                        assert_success_response(
                            res,
                            vec![vec![user_reported_by_system.to_string()]],
                        )
                        .await;
                        let res =
                            query_http_api("SHOW is_superuser", &uri, headers, configure).await;
                        let expected = if *is_superuser { "on" } else { "off" };
                        assert_success_response(res, vec![vec![expected.to_string()]]).await;
                    }
                    Assert::Err(check) => {
                        let (code, message) = match res {
                            Ok(mut res) => {
                                let body = res.body_mut().collect().await.unwrap().to_bytes();
                                let body = String::from_utf8_lossy(&body[..]).into_owned();
                                (Some(res.status()), body)
                            }
                            Err(e) => {
                                let e: &dyn std::error::Error = &e;
                                let errors = std::iter::successors(Some(e), |&e| e.source());
                                let message = errors.map(|e| e.to_string()).join(": ");
                                (None, message)
                            }
                        };
                        check(code, message)
                    }
                    Assert::DbErr(_) => unreachable!(),
                }
            }
            TestCase::Ws {
                auth,
                configure,
                assert,
            } => {
                println!("ws auth={:?}", auth);

                let uri = Uri::builder()
                    .scheme("wss")
                    .authority(&*format!(
                        "{}:{}",
                        Ipv4Addr::LOCALHOST,
                        server.http_local_addr().port()
                    ))
                    .path_and_query("/api/experimental/sql")
                    .build()
                    .unwrap();
                let stream = make_ws_tls(&uri, configure);
                let (mut ws, _resp) = tungstenite::client(uri, stream).unwrap();

                ws.send(Message::Text(serde_json::to_string(&auth).unwrap().into()))
                    .unwrap();

                ws.send(Message::Text(
                    r#"{"query": "SELECT pg_catalog.current_user()"}"#.into(),
                ))
                .unwrap();

                // Only supports reading a single row.
                fn assert_success_response(
                    ws: &mut tungstenite::WebSocket<impl Read + Write>,
                    mut expected_row_opt: Option<Vec<&str>>,
                    mut expected_tag_opt: Option<&str>,
                ) {
                    while expected_tag_opt.is_some() || expected_row_opt.is_some() {
                        let resp = ws.read().unwrap();
                        if let Message::Text(msg) = resp {
                            let msg: WebSocketResponse = serde_json::from_str(&msg).unwrap();
                            match (msg, &expected_row_opt, expected_tag_opt) {
                                (WebSocketResponse::Row(actual_row), Some(expected_row), _) => {
                                    assert_eq!(actual_row.len(), expected_row.len());
                                    for (actual_col, expected_col) in
                                        actual_row.into_iter().zip_eq(expected_row.iter())
                                    {
                                        assert_eq!(&actual_col.to_string(), expected_col);
                                    }
                                    expected_row_opt = None;
                                }
                                (
                                    WebSocketResponse::CommandComplete(actual_tag),
                                    _,
                                    Some(expected_tag),
                                ) => {
                                    assert_eq!(actual_tag, expected_tag);
                                    expected_tag_opt = None;
                                }
                                (_, _, _) => {}
                            }
                        } else {
                            panic!("unexpected: {resp}");
                        }
                    }
                }

                match assert {
                    Assert::Success => assert_success_response(&mut ws, None, Some("SELECT 1")),
                    Assert::SuccessSuperuserCheck(is_superuser) => {
                        assert_success_response(&mut ws, None, Some("SELECT 1"));
                        ws.send(Message::Text(r#"{"query": "SHOW is_superuser"}"#.into()))
                            .unwrap();
                        let expected = if *is_superuser { "\"on\"" } else { "\"off\"" };
                        assert_success_response(&mut ws, Some(vec![expected]), Some("SELECT 1"));
                    }
                    Assert::Err(check) => {
                        let resp = ws.read().unwrap();
                        let (code, message) = match resp {
                            Message::Close(frame) => {
                                let frame = frame.unwrap();
                                (frame.code, frame.reason)
                            }
                            _ => panic!("unexpected: {resp}"),
                        };
                        check(code, message.to_string())
                    }
                    Assert::DbErr(_) => unreachable!(),
                }
            }
        }
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_auth_expiry() {
    // This function verifies that the background expiry refresh task runs. This
    // is done by starting a web server that awaits the refresh request, which the
    // test waits for.

    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let email = "user@_.com".to_string();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let initial_api_tokens = vec![ApiToken {
        client_id: client_id.clone(),
        secret: secret.clone(),
        description: None,
        created_at: Utc::now(),
    }];
    let roles = Vec::new();
    let users = BTreeMap::from([(
        email.clone(),
        UserConfig {
            id: Uuid::new_v4(),
            email,
            password,
            tenant_id,
            initial_api_tokens,
            roles,
            auth_provider: None,
            verified: None,
            metadata: None,
        },
    )]);

    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();

    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        SYSTEM_TIME.clone(),
        i64::try_from(EXPIRES_IN_SECS).unwrap(),
        None,
        None,
    )
    .await
    .unwrap();

    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now: SYSTEM_TIME.clone(),
            admin_role: "mzadmin".to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );
    let frontegg_user = "user@_.com";
    let frontegg_password = &format!("mzp_{client_id}{secret}");

    let server = test_util::TestHarness::default()
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    let pg_client = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(frontegg_password)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await
        .unwrap();

    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        frontegg_user
    );

    // Wait for a couple refreshes to happen.
    frontegg_server.wait_for_auth(EXPIRES_IN_SECS);
    frontegg_server.wait_for_auth(EXPIRES_IN_SECS);
    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        frontegg_user
    );

    // Disable responding to requests.
    frontegg_server.enable_auth.store(false, Ordering::Relaxed);
    frontegg_server.wait_for_auth(EXPIRES_IN_SECS);
    // Sleep until the expiry future should resolve.
    tokio::time::sleep(Duration::from_secs(EXPIRES_IN_SECS + 1)).await;
    assert!(
        pg_client
            .query_one("SELECT current_user", &[])
            .await
            .is_err()
    );
}

#[allow(clippy::unit_arg)]
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_auth_base_require_tls_frontegg() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let initial_api_tokens = vec![ApiToken {
        client_id: client_id.clone(),
        secret: secret.clone(),
        description: None,
        created_at: Utc::now(),
    }];
    let system_password = Uuid::new_v4().to_string();
    let system_client_id = Uuid::new_v4();
    let system_secret = Uuid::new_v4();
    let system_initial_api_tokens = vec![ApiToken {
        client_id: system_client_id.clone(),
        secret: system_secret.clone(),
        description: None,
        created_at: Utc::now(),
    }];
    let service_user_client_id = Uuid::new_v4();
    let service_user_secret = Uuid::new_v4();
    let service_system_user_client_id = Uuid::new_v4();
    let service_system_user_secret = Uuid::new_v4();
    let users = BTreeMap::from([
        (
            "uSeR@_.com".to_string(),
            UserConfig {
                id: Uuid::new_v4(),
                email: "uSeR@_.com".to_string(),
                password,
                tenant_id,
                initial_api_tokens,
                roles: Vec::new(),
                auth_provider: None,
                verified: None,
                metadata: None,
            },
        ),
        (
            SYSTEM_USER.name.to_string(),
            UserConfig {
                id: Uuid::new_v4(),
                email: SYSTEM_USER.name.to_string(),
                password: system_password,
                tenant_id,
                initial_api_tokens: system_initial_api_tokens,
                roles: Vec::new(),
                auth_provider: None,
                verified: None,
                metadata: None,
            },
        ),
    ]);
    let tenant_api_tokens = BTreeMap::from([
        (
            ApiToken {
                client_id: service_user_client_id.clone(),
                secret: service_user_secret.clone(),
                description: None,
                created_at: Utc::now(),
            },
            TenantApiTokenConfig {
                tenant_id,
                roles: vec![],
                metadata: Some(ClaimMetadata {
                    user: Some("svc".into()),
                }),
                description: None,
                created_at: Utc::now(),
                created_by_user_id: client_id,
            },
        ),
        (
            ApiToken {
                client_id: service_system_user_client_id.clone(),
                secret: service_system_user_secret.clone(),
                description: None,
                created_at: Utc::now(),
            },
            TenantApiTokenConfig {
                tenant_id,
                roles: vec![],
                metadata: Some(ClaimMetadata {
                    user: Some("mz_system".into()),
                }),
                description: None,
                created_at: Utc::now(),
                created_by_user_id: client_id,
            },
        ),
    ]);
    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();
    let timestamp = Arc::new(Mutex::new(500_000));
    let now = {
        let timestamp = Arc::clone(&timestamp);
        NowFn::from(move || *timestamp.lock().unwrap())
    };
    let claims = Claims {
        exp: 1000,
        email: Some("uSeR@_.com".to_string()),
        iss: "frontegg-mock".to_string(),
        sub: Uuid::new_v4(),
        user_id: None,
        tenant_id,
        roles: Vec::new(),
        permissions: Vec::new(),
        token_type: ClaimTokenType::UserToken,
        metadata: None,
    };
    let frontegg_jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        &encoding_key,
    )
    .unwrap();
    let user_set_metadata_claims = Claims {
        metadata: Some(ClaimMetadata {
            user: Some("svc".into()),
        }),
        ..claims.clone()
    };
    let user_set_metadata_jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &user_set_metadata_claims,
        &encoding_key,
    )
    .unwrap();
    let bad_tenant_claims = Claims {
        tenant_id: Uuid::new_v4(),
        ..claims.clone()
    };
    let bad_tenant_jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &bad_tenant_claims,
        &encoding_key,
    )
    .unwrap();
    let expired_claims = Claims {
        exp: 0,
        ..claims.clone()
    };
    let expired_jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &expired_claims,
        &encoding_key,
    )
    .unwrap();
    let service_system_user_claims = Claims {
        token_type: ClaimTokenType::TenantApiToken,
        email: None,
        metadata: Some(ClaimMetadata {
            user: Some("mz_system".into()),
        }),
        ..claims.clone()
    };
    let service_system_user_jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &service_system_user_claims,
        &encoding_key,
    )
    .unwrap();
    let service_user_claims = Claims {
        token_type: ClaimTokenType::TenantApiToken,
        email: None,
        metadata: Some(ClaimMetadata {
            user: Some("svc".into()),
        }),
        ..claims.clone()
    };
    let service_user_jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &service_user_claims,
        &encoding_key,
    )
    .unwrap();
    let bad_service_user_claims = Claims {
        token_type: ClaimTokenType::TenantApiToken,
        email: None,
        metadata: Some(ClaimMetadata {
            user: Some("svc@corp".into()),
        }),
        ..claims.clone()
    };
    let bad_service_user_jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &bad_service_user_claims,
        &encoding_key,
    )
    .unwrap();
    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        tenant_api_tokens,
        None,
        now.clone(),
        1_000,
        None,
        None,
    )
    .await
    .unwrap();

    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now,
            admin_role: "mzadmin".to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );
    let frontegg_user = "uSeR@_.com";
    let frontegg_password = &format!("mzp_{client_id}{secret}");
    let frontegg_basic = Authorization::basic(frontegg_user, frontegg_password);
    let frontegg_header_basic = make_header(frontegg_basic);

    let frontegg_user_lowercase = frontegg_user.to_lowercase();
    let frontegg_basic_lowercase =
        Authorization::basic(&frontegg_user_lowercase, frontegg_password);
    let frontegg_header_basic_lowercase = make_header(frontegg_basic_lowercase);

    let frontegg_system_password = &format!("mzp_{system_client_id}{system_secret}");
    let frontegg_system_basic = Authorization::basic(&SYSTEM_USER.name, frontegg_system_password);
    let frontegg_system_header_basic = make_header(frontegg_system_basic);

    let frontegg_service_user_password =
        &format!("mzp_{service_user_client_id}{service_user_secret}");

    let frontegg_service_system_user_password =
        &format!("mzp_{service_system_user_client_id}{service_system_user_secret}");

    let no_headers = HeaderMap::new();

    // Test connecting to a server that requires TLS and uses Materialize Cloud for
    // authentication.
    let server = test_util::TestHarness::default()
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    run_tests(
        "TlsMode::Require, MzCloud",
        &server,
        &[
            TestCase::Ws {
                auth: &WebSocketAuth::Basic {
                    user: frontegg_user.to_string(),
                    password: Password(frontegg_password.to_string()),
                    options: BTreeMap::default(),
                },
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            TestCase::Ws {
                auth: &WebSocketAuth::Bearer {
                    token: frontegg_jwt.clone(),
                    options: BTreeMap::default(),
                },
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            TestCase::Ws {
                auth: &WebSocketAuth::Basic {
                    user: "bad user".to_string(),
                    password: Password(frontegg_password.to_string()),
                    options: BTreeMap::default(),
                },
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, CloseCode::Protocol);
                    assert_eq!(message, "unauthorized");
                })),
            },
            // TLS with a password should succeed.
            TestCase::Pgwire {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                password: Some(Cow::Borrowed(frontegg_password)),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            TestCase::Http {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &frontegg_header_basic,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Email comparisons should be case insensitive.
            TestCase::Pgwire {
                user_to_auth_as: &frontegg_user_lowercase,
                user_reported_by_system: frontegg_user,
                password: Some(Cow::Borrowed(frontegg_password)),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            TestCase::Http {
                user_to_auth_as: &frontegg_user_lowercase,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &frontegg_header_basic_lowercase,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            TestCase::Ws {
                auth: &WebSocketAuth::Basic {
                    user: frontegg_user_lowercase.to_string(),
                    password: Password(frontegg_password.to_string()),
                    options: BTreeMap::default(),
                },
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Password can be base64 encoded UUID bytes.
            TestCase::Pgwire {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                password: {
                    let mut buf = vec![];
                    buf.extend(client_id.as_bytes());
                    buf.extend(secret.as_bytes());
                    Some(Cow::Owned(format!("mzp_{}", URL_SAFE.encode(buf))))
                },
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Password can be base64 encoded UUID bytes without padding.
            TestCase::Pgwire {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                password: {
                    let mut buf = vec![];
                    buf.extend(client_id.as_bytes());
                    buf.extend(secret.as_bytes());
                    Some(Cow::Owned(format!("mzp_{}", URL_SAFE_NO_PAD.encode(buf))))
                },
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Password can include arbitrary special characters.
            TestCase::Pgwire {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                password: {
                    let mut password = frontegg_password.clone();
                    password.insert(10, '-');
                    password.insert_str(15, "@#!");
                    Some(Cow::Owned(password.clone()))
                },
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Bearer auth doesn't need the clientid or secret.
            TestCase::Http {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::bearer(&frontegg_jwt).unwrap()),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // No TLS fails.
            TestCase::Pgwire {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                password: Some(Cow::Borrowed(frontegg_password)),
                ssl_mode: SslMode::Disable,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::DbErr(Box::new(|err| {
                    assert_eq!(
                        *err.code(),
                        SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
                    );
                    assert_eq!(err.message(), "TLS encryption is required");
                })),
            },
            TestCase::Http {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTP,
                headers: &frontegg_header_basic,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: assert_http_rejected(),
            },
            // Wrong, but existing, username.
            TestCase::Pgwire {
                user_to_auth_as: "materialize",
                user_reported_by_system: "materialize",
                password: Some(Cow::Borrowed(frontegg_password)),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::DbErr(Box::new(|err| {
                    assert_eq!(err.message(), "invalid password");
                    assert_eq!(*err.code(), SqlState::INVALID_PASSWORD);
                })),
            },
            TestCase::Http {
                user_to_auth_as: "materialize",
                user_reported_by_system: "materialize",
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::basic("materialize", frontegg_password)),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "unauthorized");
                })),
            },
            // Wrong password.
            TestCase::Pgwire {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                password: Some(Cow::Borrowed("bad password")),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::DbErr(Box::new(|err| {
                    assert_eq!(err.message(), "invalid password");
                    assert_eq!(*err.code(), SqlState::INVALID_PASSWORD);
                })),
            },
            TestCase::Http {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::basic(frontegg_user, "bad password")),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "unauthorized");
                })),
            },
            // Bad password prefix.
            TestCase::Pgwire {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                password: Some(Cow::Owned(format!("mznope_{client_id}{secret}"))),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::DbErr(Box::new(|err| {
                    assert_eq!(err.message(), "invalid password");
                    assert_eq!(*err.code(), SqlState::INVALID_PASSWORD);
                })),
            },
            TestCase::Http {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::basic(
                    frontegg_user,
                    &format!("mznope_{client_id}{secret}"),
                )),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "unauthorized");
                })),
            },
            // No password.
            TestCase::Pgwire {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::DbErr(Box::new(|err| {
                    assert_eq!(err.message(), "invalid password");
                    assert_eq!(*err.code(), SqlState::INVALID_PASSWORD);
                })),
            },
            TestCase::Http {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "unauthorized");
                })),
            },
            // Bad auth scheme
            TestCase::Http {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &HeaderMap::from_iter(vec![(
                    AUTHORIZATION,
                    HeaderValue::from_static("Digest username=materialize"),
                )]),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "unauthorized");
                })),
            },
            // Bad tenant.
            TestCase::Http {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::bearer(&bad_tenant_jwt).unwrap()),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "unauthorized");
                })),
            },
            // Expired.
            TestCase::Http {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::bearer(&expired_jwt).unwrap()),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "unauthorized");
                })),
            },
            // Valid service users.
            TestCase::Http {
                user_to_auth_as: "svc",
                user_reported_by_system: "svc",
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::bearer(&service_user_jwt).unwrap()),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            TestCase::Pgwire {
                user_to_auth_as: "svc",
                user_reported_by_system: "svc",
                password: Some(Cow::Borrowed(frontegg_service_user_password)),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Service users are ignored in user tokens.
            TestCase::Http {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::bearer(&user_set_metadata_jwt).unwrap()),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Service user using email address is rejected.
            TestCase::Http {
                user_to_auth_as: "svc@corp",
                user_reported_by_system: "svc@corp",
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::bearer(&bad_service_user_jwt).unwrap()),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "unauthorized");
                })),
            },
            // Service user using invalid case is rejected.
            TestCase::Pgwire {
                user_to_auth_as: "sVc",
                user_reported_by_system: "svc",
                password: Some(Cow::Borrowed(frontegg_service_user_password)),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::DbErr(Box::new(|err| {
                    assert_eq!(err.message(), "invalid password");
                    assert_eq!(*err.code(), SqlState::INVALID_PASSWORD);
                })),
            },
            // System user cannot login via external ports.
            TestCase::Pgwire {
                user_to_auth_as: &*SYSTEM_USER.name,
                user_reported_by_system: &*SYSTEM_USER.name,
                password: Some(Cow::Borrowed(frontegg_system_password)),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|err| {
                    assert_contains!(
                        err.to_string_with_causes(),
                        "unauthorized login to user 'mz_system'"
                    );
                })),
            },
            TestCase::Http {
                user_to_auth_as: &*SYSTEM_USER.name,
                user_reported_by_system: &*SYSTEM_USER.name,
                scheme: Scheme::HTTPS,
                headers: &frontegg_system_header_basic,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_contains!(message, "unauthorized");
                })),
            },
            TestCase::Ws {
                auth: &WebSocketAuth::Basic {
                    user: (&*SYSTEM_USER.name).into(),
                    password: Password(frontegg_system_password.to_string()),
                    options: BTreeMap::default(),
                },
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, CloseCode::Protocol);
                    assert_eq!(message, "unauthorized");
                })),
            },
            TestCase::Pgwire {
                user_to_auth_as: &*SYSTEM_USER.name,
                user_reported_by_system: &*SYSTEM_USER.name,
                password: Some(Cow::Borrowed(frontegg_service_system_user_password)),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|err| {
                    assert_contains!(
                        err.to_string_with_causes(),
                        "unauthorized login to user 'mz_system'"
                    );
                })),
            },
            TestCase::Http {
                user_to_auth_as: &*SYSTEM_USER.name,
                user_reported_by_system: &*SYSTEM_USER.name,
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::bearer(&service_system_user_jwt).unwrap()),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_contains!(message, "unauthorized");
                })),
            },
            TestCase::Ws {
                auth: &WebSocketAuth::Basic {
                    user: (&*SYSTEM_USER.name).into(),
                    password: Password(frontegg_service_system_user_password.to_string()),
                    options: BTreeMap::default(),
                },
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, CloseCode::Protocol);
                    assert_eq!(message, "unauthorized");
                })),
            },
            // Public role cannot login.
            TestCase::Pgwire {
                user_to_auth_as: PUBLIC_ROLE_NAME.as_str(),
                user_reported_by_system: PUBLIC_ROLE_NAME.as_str(),
                password: Some(Cow::Borrowed(frontegg_system_password)),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|err| {
                    assert_contains!(
                        err.to_string_with_causes(),
                        "unauthorized login to user 'PUBLIC'"
                    );
                })),
            },
            TestCase::Http {
                user_to_auth_as: PUBLIC_ROLE_NAME.as_str(),
                user_reported_by_system: PUBLIC_ROLE_NAME.as_str(),
                scheme: Scheme::HTTPS,
                headers: &frontegg_system_header_basic,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_contains!(message, "unauthorized");
                })),
            },
            TestCase::Ws {
                auth: &WebSocketAuth::Basic {
                    user: (PUBLIC_ROLE_NAME.as_str()).into(),
                    password: Password(frontegg_system_password.to_string()),
                    options: BTreeMap::default(),
                },
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, CloseCode::Protocol);
                    assert_eq!(message, "unauthorized");
                })),
            },
        ],
    )
    .await;
}

#[allow(clippy::unit_arg)]
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_auth_base_disable_tls() {
    let no_headers = HeaderMap::new();

    // Test TLS modes with a server that does not support TLS.
    let server = test_util::TestHarness::default().start().await;
    run_tests(
        "TlsMode::Disable",
        &server,
        &[
            // Explicitly disabling TLS should succeed.
            TestCase::Pgwire {
                user_to_auth_as: "materialize",
                user_reported_by_system: "materialize",
                password: None,
                ssl_mode: SslMode::Disable,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Success,
            },
            TestCase::Http {
                user_to_auth_as: &*HTTP_DEFAULT_USER.name,
                user_reported_by_system: &*HTTP_DEFAULT_USER.name,
                scheme: Scheme::HTTP,
                headers: &no_headers,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Success,
            },
            // Preferring TLS should fall back to no TLS.
            TestCase::Pgwire {
                user_to_auth_as: "materialize",
                user_reported_by_system: "materialize",
                password: None,
                ssl_mode: SslMode::Prefer,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Success,
            },
            // Requiring TLS should fail.
            TestCase::Pgwire {
                user_to_auth_as: "materialize",
                user_reported_by_system: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Err(Box::new(|err| {
                    assert_eq!(
                        err.to_string_with_causes(),
                        "error performing TLS handshake: server does not support TLS",
                    )
                })),
            },
            TestCase::Http {
                user_to_auth_as: &*HTTP_DEFAULT_USER.name,
                user_reported_by_system: &*HTTP_DEFAULT_USER.name,
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Err(Box::new(|code, message| {
                    // Connecting to an HTTP server via HTTPS does not yield
                    // a graceful error message. This could plausibly change
                    // due to OpenSSL or Hyper refactorings.
                    assert_none!(code);
                    assert_contains!(message, "packet length too long");
                })),
            },
            // System user cannot login via external ports.
            TestCase::Pgwire {
                user_to_auth_as: &*SYSTEM_USER.name,
                user_reported_by_system: &*SYSTEM_USER.name,
                password: None,
                ssl_mode: SslMode::Disable,
                configure: Box::new(|_| Ok(())),
                assert: Assert::DbErr(Box::new(|err| {
                    assert_contains!(
                        err.to_string_with_causes(),
                        "unauthorized login to user 'mz_system'"
                    );
                })),
            },
        ],
    )
    .await;
}

#[allow(clippy::unit_arg)]
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_auth_base_require_tls() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();

    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let frontegg_user = "uSeR@_.com";
    let frontegg_password = &format!("mzp_{client_id}{secret}");
    let frontegg_basic = Authorization::basic(frontegg_user, frontegg_password);
    let frontegg_header_basic = make_header(frontegg_basic);

    let no_headers = HeaderMap::new();

    // Test TLS modes with a server that requires TLS.
    let server = test_util::TestHarness::default()
        .with_tls(server_cert, server_key)
        .start()
        .await;

    run_tests(
        "TlsMode::Require",
        &server,
        &[
            // Non-existent role will be created.
            TestCase::Pgwire {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                password: Some(Cow::Borrowed(frontegg_password)),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Test that specifies a username/password in the mzcloud header should use the username
            TestCase::Http {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &frontegg_header_basic,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Test that has no headers should use the default user
            TestCase::Http {
                user_to_auth_as: &*HTTP_DEFAULT_USER.name,
                user_reported_by_system: &*HTTP_DEFAULT_USER.name,
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Disabling TLS should fail.
            TestCase::Pgwire {
                user_to_auth_as: "materialize",
                user_reported_by_system: "materialize",
                password: None,
                ssl_mode: SslMode::Disable,
                configure: Box::new(|_| Ok(())),
                assert: Assert::DbErr(Box::new(|err| {
                    assert_eq!(
                        *err.code(),
                        SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
                    );
                    assert_eq!(err.message(), "TLS encryption is required");
                })),
            },
            TestCase::Http {
                user_to_auth_as: &*HTTP_DEFAULT_USER.name,
                user_reported_by_system: &*HTTP_DEFAULT_USER.name,
                scheme: Scheme::HTTP,
                headers: &no_headers,
                configure: Box::new(|_| Ok(())),
                assert: assert_http_rejected(),
            },
            // Preferring TLS should succeed.
            TestCase::Pgwire {
                user_to_auth_as: "materialize",
                user_reported_by_system: "materialize",
                password: None,
                ssl_mode: SslMode::Prefer,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Requiring TLS should succeed.
            TestCase::Pgwire {
                user_to_auth_as: "materialize",
                user_reported_by_system: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            TestCase::Http {
                user_to_auth_as: &*HTTP_DEFAULT_USER.name,
                user_reported_by_system: &*HTTP_DEFAULT_USER.name,
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // System user cannot login via external ports.
            TestCase::Pgwire {
                user_to_auth_as: &*SYSTEM_USER.name,
                user_reported_by_system: &*SYSTEM_USER.name,
                password: None,
                ssl_mode: SslMode::Prefer,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::DbErr(Box::new(|err| {
                    assert_contains!(
                        err.to_string_with_causes(),
                        "unauthorized login to user 'mz_system'"
                    );
                })),
            },
        ],
    )
    .await;
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_auth_intermediate_ca_no_intermediary() {
    // Create a CA, an intermediate CA, and a server key pair signed by the
    // intermediate CA.
    let ca = Ca::new_root("test ca").unwrap();
    let intermediate_ca = ca.request_ca("intermediary").unwrap();
    let (server_cert, server_key) = intermediate_ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();

    // When the server presents only its own certificate, without the
    // intermediary, the client should fail to verify the chain.
    let server = test_util::TestHarness::default()
        .with_tls(server_cert, server_key)
        .start()
        .await;

    run_tests(
        "TlsMode::Require",
        &server,
        &[
            TestCase::Pgwire {
                user_to_auth_as: "materialize",
                user_reported_by_system: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| b.set_ca_file(ca.ca_cert_path())),
                assert: Assert::Err(Box::new(|err| {
                    assert_contains!(
                        err.to_string_with_causes(),
                        "unable to get local issuer certificate"
                    );
                })),
            },
            TestCase::Http {
                user_to_auth_as: &*HTTP_DEFAULT_USER.name,
                user_reported_by_system: &*HTTP_DEFAULT_USER.name,
                scheme: Scheme::HTTPS,
                headers: &HeaderMap::new(),
                configure: Box::new(|b| b.set_ca_file(ca.ca_cert_path())),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_none!(code);
                    assert_contains!(message, "unable to get local issuer certificate");
                })),
            },
        ],
    )
    .await;
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_auth_intermediate_ca() {
    // Create a CA, an intermediate CA, and a server key pair signed by the
    // intermediate CA.
    let ca = Ca::new_root("test ca").unwrap();
    let intermediate_ca = ca.request_ca("intermediary").unwrap();
    let (server_cert, server_key) = intermediate_ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();

    // Create a certificate chain bundle that contains the server's certificate
    // and the intermediate CA's certificate.
    let server_cert_chain = {
        let path = intermediate_ca.dir.path().join("server.chain.crt");
        let mut buf = vec![];
        File::open(server_cert)
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();
        File::open(intermediate_ca.ca_cert_path())
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();
        fs::write(&path, buf).unwrap();
        path
    };

    // When the server is configured to present the entire certificate chain,
    // the client should be able to verify the chain even though it only knows
    // about the root CA.
    let server = test_util::TestHarness::default()
        .with_tls(server_cert_chain, server_key)
        .start()
        .await;

    run_tests(
        "TlsMode::Require",
        &server,
        &[
            TestCase::Pgwire {
                user_to_auth_as: "materialize",
                user_reported_by_system: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| b.set_ca_file(ca.ca_cert_path())),
                assert: Assert::Success,
            },
            TestCase::Http {
                user_to_auth_as: &*HTTP_DEFAULT_USER.name,
                user_reported_by_system: &*HTTP_DEFAULT_USER.name,
                scheme: Scheme::HTTPS,
                headers: &HeaderMap::new(),
                configure: Box::new(|b| b.set_ca_file(ca.ca_cert_path())),
                assert: Assert::Success,
            },
        ],
    )
    .await;
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_auth_admin_non_superuser() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let admin_password = Uuid::new_v4().to_string();
    let admin_client_id = Uuid::new_v4();
    let admin_secret = Uuid::new_v4();

    let frontegg_user = "user@_.com";
    let admin_frontegg_user = "admin@_.com";
    let created_at = Utc::now();

    let admin_role = "mzadmin";

    let users = BTreeMap::from([
        (
            frontegg_user.to_string(),
            UserConfig {
                id: Uuid::new_v4(),
                email: frontegg_user.to_string(),
                password,
                tenant_id,
                initial_api_tokens: vec![ApiToken {
                    client_id,
                    secret,
                    description: None,
                    created_at,
                }],
                roles: Vec::new(),
                auth_provider: None,
                verified: None,
                metadata: None,
            },
        ),
        (
            admin_frontegg_user.to_string(),
            UserConfig {
                id: Uuid::new_v4(),
                email: admin_frontegg_user.to_string(),
                password: admin_password,
                tenant_id,
                initial_api_tokens: vec![ApiToken {
                    client_id: admin_client_id,
                    secret: admin_secret,
                    description: None,
                    created_at: Utc::now(),
                }],
                roles: vec![admin_role.to_string()],
                auth_provider: None,
                verified: None,
                metadata: None,
            },
        ),
    ]);
    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();
    let now = SYSTEM_TIME.clone();

    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        now.clone(),
        i64::try_from(EXPIRES_IN_SECS).unwrap(),
        None,
        None,
    )
    .await
    .unwrap();

    let password_prefix = "mzp_";
    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now,
            admin_role: admin_role.to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );

    let frontegg_password = &format!("{password_prefix}{client_id}{secret}");
    let frontegg_basic = Authorization::basic(frontegg_user, frontegg_password);
    let frontegg_header_basic = make_header(frontegg_basic);

    let server = test_util::TestHarness::default()
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    run_tests(
        "Non-superuser",
        &server,
        &[
            TestCase::Pgwire {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                password: Some(Cow::Borrowed(frontegg_password)),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::SuccessSuperuserCheck(false),
            },
            TestCase::Http {
                user_to_auth_as: frontegg_user,
                user_reported_by_system: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &frontegg_header_basic,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::SuccessSuperuserCheck(false),
            },
            TestCase::Ws {
                auth: &WebSocketAuth::Basic {
                    user: frontegg_user.to_string(),
                    password: Password(frontegg_password.to_string()),
                    options: BTreeMap::default(),
                },
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::SuccessSuperuserCheck(false),
            },
        ],
    )
    .await;
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_auth_admin_superuser() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let admin_password = Uuid::new_v4().to_string();
    let admin_client_id = Uuid::new_v4();
    let admin_secret = Uuid::new_v4();

    let frontegg_user = "user@_.com";
    let admin_frontegg_user = "admin@_.com";
    let created_at = Utc::now();

    let admin_role = "mzadmin";

    let users = BTreeMap::from([
        (
            frontegg_user.to_string(),
            UserConfig {
                id: Uuid::new_v4(),
                email: frontegg_user.to_string(),
                password,
                tenant_id,
                initial_api_tokens: vec![ApiToken {
                    client_id,
                    secret,
                    description: None,
                    created_at,
                }],
                roles: Vec::new(),
                auth_provider: None,
                verified: None,
                metadata: None,
            },
        ),
        (
            admin_frontegg_user.to_string(),
            UserConfig {
                id: Uuid::new_v4(),
                email: admin_frontegg_user.to_string(),
                password: admin_password,
                tenant_id,
                initial_api_tokens: vec![ApiToken {
                    client_id: admin_client_id,
                    secret: admin_secret,
                    description: None,
                    created_at: Utc::now(),
                }],
                roles: vec![admin_role.to_string()],
                auth_provider: None,
                verified: None,
                metadata: None,
            },
        ),
    ]);
    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();
    let now = SYSTEM_TIME.clone();

    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        now.clone(),
        i64::try_from(EXPIRES_IN_SECS).unwrap(),
        None,
        None,
    )
    .await
    .unwrap();

    let password_prefix = "mzp_";
    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now,
            admin_role: admin_role.to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );

    let admin_frontegg_password = &format!("{password_prefix}{admin_client_id}{admin_secret}");
    let admin_frontegg_basic = Authorization::basic(admin_frontegg_user, admin_frontegg_password);
    let admin_frontegg_header_basic = make_header(admin_frontegg_basic);

    let server = test_util::TestHarness::default()
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    run_tests(
        "Superuser",
        &server,
        &[
            TestCase::Pgwire {
                user_to_auth_as: admin_frontegg_user,
                user_reported_by_system: admin_frontegg_user,
                password: Some(Cow::Borrowed(admin_frontegg_password)),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::SuccessSuperuserCheck(true),
            },
            TestCase::Http {
                user_to_auth_as: admin_frontegg_user,
                user_reported_by_system: admin_frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &admin_frontegg_header_basic,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::SuccessSuperuserCheck(true),
            },
            TestCase::Ws {
                auth: &WebSocketAuth::Basic {
                    user: admin_frontegg_user.to_string(),
                    password: Password(admin_frontegg_password.to_string()),
                    options: BTreeMap::default(),
                },
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::SuccessSuperuserCheck(true),
            },
        ],
    )
    .await;
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_auth_admin_superuser_revoked() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let admin_password = Uuid::new_v4().to_string();
    let admin_client_id = Uuid::new_v4();
    let admin_secret = Uuid::new_v4();

    let frontegg_user = "user@_.com";
    let admin_frontegg_user = "admin@_.com";
    let created_at = Utc::now();

    let admin_role = "mzadmin";

    let users = BTreeMap::from([
        (
            frontegg_user.to_string(),
            UserConfig {
                id: Uuid::new_v4(),
                email: frontegg_user.to_string(),
                password,
                tenant_id,
                initial_api_tokens: vec![ApiToken {
                    client_id,
                    secret,
                    description: None,
                    created_at,
                }],
                roles: Vec::new(),
                auth_provider: None,
                verified: None,
                metadata: None,
            },
        ),
        (
            admin_frontegg_user.to_string(),
            UserConfig {
                id: Uuid::new_v4(),
                email: admin_frontegg_user.to_string(),
                password: admin_password,
                tenant_id,
                initial_api_tokens: vec![ApiToken {
                    client_id: admin_client_id,
                    secret: admin_secret,
                    description: None,
                    created_at: Utc::now(),
                }],
                roles: vec![admin_role.to_string()],
                auth_provider: None,
                verified: None,
                metadata: None,
            },
        ),
    ]);
    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();
    let now = SYSTEM_TIME.clone();

    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        now.clone(),
        i64::try_from(EXPIRES_IN_SECS).unwrap(),
        None,
        None,
    )
    .await
    .unwrap();

    let password_prefix = "mzp_";
    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now,
            admin_role: admin_role.to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );

    let frontegg_password = &format!("{password_prefix}{client_id}{secret}");

    let server = test_util::TestHarness::default()
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    let pg_client = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(frontegg_password)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await
        .unwrap();

    assert_eq!(
        pg_client
            .query_one("SHOW is_superuser", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "off"
    );

    frontegg_server
        .role_updates_tx
        .send((frontegg_user.to_string(), vec![admin_role.to_string()]))
        .unwrap();
    frontegg_server.wait_for_auth(EXPIRES_IN_SECS);

    assert_eq!(
        pg_client
            .query_one("SHOW is_superuser", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "on"
    );

    frontegg_server
        .role_updates_tx
        .send((frontegg_user.to_string(), Vec::new()))
        .unwrap();
    frontegg_server.wait_for_auth(EXPIRES_IN_SECS);

    assert_eq!(
        pg_client
            .query_one("SHOW is_superuser", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "off"
    );
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_auth_deduplication() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let created_at = Utc::now();

    let frontegg_user = "user@_.com";

    let users = BTreeMap::from([(
        frontegg_user.to_string(),
        UserConfig {
            id: Uuid::new_v4(),
            email: frontegg_user.to_string(),
            password,
            tenant_id,
            initial_api_tokens: vec![ApiToken {
                client_id,
                secret,
                description: None,
                created_at,
            }],
            roles: Vec::new(),
            auth_provider: None,
            verified: None,
            metadata: None,
        },
    )]);
    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();
    let now = SYSTEM_TIME.clone();

    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        now.clone(),
        i64::try_from(EXPIRES_IN_SECS).unwrap(),
        None,
        None,
    )
    .await
    .unwrap();

    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now: SYSTEM_TIME.clone(),
            admin_role: "mzadmin".to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );
    let frontegg_user = "user@_.com";
    let frontegg_password = &format!("mzp_{client_id}{secret}");

    let server = test_util::TestHarness::default()
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 0);

    let pg_client_1_fut = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(frontegg_password)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .into_future();

    let pg_client_2_fut = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(frontegg_password)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .into_future();

    let (client_1_result, client_2_result) =
        futures::future::join(pg_client_1_fut, pg_client_2_fut).await;
    let pg_client_1 = client_1_result.unwrap();
    let pg_client_2 = client_2_result.unwrap();

    // We should have de-duplicated the request and only actually sent 1.
    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 1);

    let frontegg_user_client_1 = pg_client_1
        .query_one("SELECT current_user", &[])
        .await
        .unwrap()
        .get::<_, String>(0);
    assert_eq!(frontegg_user_client_1, frontegg_user);

    let frontegg_user_client_2 = pg_client_2
        .query_one("SELECT current_user", &[])
        .await
        .unwrap()
        .get::<_, String>(0);
    assert_eq!(frontegg_user_client_2, frontegg_user);

    // Wait for a refresh to occur.
    frontegg_server.wait_for_auth(10);
    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 2);

    // Our metrics should reflect we attached to a pending session.
    let metrics = server.metrics_registry.gather();
    let mut metrics: Vec<_> = metrics
        .into_iter()
        .filter(|family| family.name() == "mz_auth_session_request_count")
        .collect();
    assert_eq!(metrics.len(), 1);
    let metric = metrics.pop().unwrap();

    let metrics = metric.get_metric();
    assert_eq!(metrics.len(), 2);

    let metric = &metrics[0];
    assert_eq!(metric.get_counter().get_value(), 1.0);
    let labels = metric.get_label();
    assert_eq!(labels.len(), 1);
    let reason_1 = labels[0].value().to_string();

    let metric = &metrics[1];
    assert_eq!(metric.get_counter().get_value(), 1.0);
    let labels = metric.get_label();
    assert_eq!(labels.len(), 1);
    let reason_2 = labels[0].value().to_string();

    // One reason should indicate a new session, while the other indicates joining an existing session.
    assert_ne!(reason_1, reason_2);

    // Both clients should still be queryable.
    let frontegg_user_client_1_post_refresh = pg_client_1
        .query_one("SELECT current_user", &[])
        .await
        .unwrap()
        .get::<_, String>(0);
    assert_eq!(frontegg_user_client_1_post_refresh, frontegg_user);

    let frontegg_user_client_2_post_refresh = pg_client_2
        .query_one("SELECT current_user", &[])
        .await
        .unwrap()
        .get::<_, String>(0);
    assert_eq!(frontegg_user_client_2_post_refresh, frontegg_user);
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_refresh_task_metrics() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let created_at = Utc::now();

    let frontegg_user = "user@_.com";

    let users = BTreeMap::from([(
        frontegg_user.to_string(),
        UserConfig {
            id: Uuid::new_v4(),
            email: frontegg_user.to_string(),
            password,
            tenant_id,
            initial_api_tokens: vec![ApiToken {
                client_id,
                secret,
                description: None,
                created_at,
            }],
            roles: Vec::new(),
            auth_provider: None,
            verified: None,
            metadata: None,
        },
    )]);
    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();
    let now = SYSTEM_TIME.clone();

    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        now.clone(),
        i64::try_from(EXPIRES_IN_SECS).unwrap(),
        None,
        None,
    )
    .await
    .unwrap();

    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now: SYSTEM_TIME.clone(),
            admin_role: "mzadmin".to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );
    let frontegg_user = "user@_.com";
    let frontegg_password = &format!("mzp_{client_id}{secret}");

    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "log_filter".to_string(),
            "mz_frontegg_auth=debug,info".to_string(),
        )
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    let pg_client = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(frontegg_password)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await
        .unwrap();

    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        frontegg_user
    );

    // Make sure our guage indicates there is one refresh task running.
    let metrics = server.metrics_registry.gather();
    let mut metrics: Vec<_> = metrics
        .into_iter()
        .filter(|family| family.name() == "mz_auth_refresh_tasks_active")
        .collect();
    assert_eq!(metrics.len(), 1);
    let metric = metrics.pop().unwrap();
    let metric = &metric.get_metric()[0];
    assert_eq!(metric.get_gauge().get_value(), 1.0);

    drop(pg_client);

    // The refresh task won't shut down until the auth has expired, so we need to wait for that to
    // occur.
    sleep(Duration::from_secs(EXPIRES_IN_SECS)).await;

    // Client is dropped, auth has expired, we should not have any refresh tasks running.
    let metrics = server.metrics_registry.gather();
    let mut metrics: Vec<_> = metrics
        .into_iter()
        .filter(|family| family.name() == "mz_auth_refresh_tasks_active")
        .collect();

    let metric = metrics.pop().unwrap();
    let metric = &metric.get_metric()[0];

    let guage_value = metric.get_gauge().get_value();
    assert_eq!(guage_value, 0.0);
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_superuser_can_alter_cluster() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let admin_password = Uuid::new_v4().to_string();
    let admin_client_id = Uuid::new_v4();
    let admin_secret = Uuid::new_v4();
    let created_at = Utc::now();

    let frontegg_user = "user@_.com";
    let admin_frontegg_user = "admin@_.com";

    let admin_role = "mzadmin";

    let users = BTreeMap::from([
        (
            frontegg_user.to_string(),
            UserConfig {
                id: Uuid::new_v4(),
                email: frontegg_user.to_string(),
                password,
                tenant_id,
                initial_api_tokens: vec![ApiToken {
                    client_id,
                    secret,
                    description: None,
                    created_at,
                }],
                roles: Vec::new(),
                auth_provider: None,
                verified: None,
                metadata: None,
            },
        ),
        (
            admin_frontegg_user.to_string(),
            UserConfig {
                id: Uuid::new_v4(),
                email: admin_frontegg_user.to_string(),
                password: admin_password.clone(),
                tenant_id,
                initial_api_tokens: vec![ApiToken {
                    client_id: admin_client_id,
                    secret: admin_secret,
                    description: None,
                    created_at: Utc::now(),
                }],
                roles: vec![admin_role.to_string()],
                auth_provider: None,
                verified: None,
                metadata: None,
            },
        ),
    ]);
    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();
    let now = SYSTEM_TIME.clone();

    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        now.clone(),
        i64::try_from(EXPIRES_IN_SECS).unwrap(),
        None,
        None,
    )
    .await
    .unwrap();

    let password_prefix = "mzp_";
    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now,
            admin_role: admin_role.to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );

    let admin_frontegg_password = format!("{password_prefix}{admin_client_id}{admin_secret}");
    let frontegg_user_password = format!("{password_prefix}{client_id}{secret}");

    let server = test_util::TestHarness::default()
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    let tls = make_pg_tls(|b| Ok(b.set_verify(SslVerifyMode::NONE)));
    let superuser = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(admin_frontegg_user)
        .password(&admin_frontegg_password)
        .with_tls(tls.clone())
        .await
        .unwrap();

    let default_cluster = superuser
        .query_one("SHOW cluster", &[])
        .await
        .unwrap()
        .get::<_, String>(0);
    assert_eq!(default_cluster, "quickstart");

    // External admins should be able to modify the system default cluster.
    superuser
        .execute("ALTER SYSTEM SET cluster TO foo_bar", &[])
        .await
        .unwrap();

    // New system defaults only take effect for new sessions.
    let regular_user = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(&frontegg_user_password)
        .with_tls(tls)
        .await
        .unwrap();

    let new_default_cluster = regular_user
        .query_one("SHOW cluster", &[])
        .await
        .unwrap()
        .get::<_, String>(0);
    assert_eq!(new_default_cluster, "foo_bar");
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_refresh_dropped_session() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let created_at = Utc::now();

    let frontegg_user = "user@_.com";

    let users = BTreeMap::from([(
        frontegg_user.to_string(),
        UserConfig {
            id: Uuid::new_v4(),
            email: frontegg_user.to_string(),
            password,
            tenant_id,
            initial_api_tokens: vec![ApiToken {
                client_id,
                secret,
                description: None,
                created_at,
            }],
            roles: Vec::new(),
            auth_provider: None,
            verified: None,
            metadata: None,
        },
    )]);
    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();
    let now = SYSTEM_TIME.clone();

    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        now.clone(),
        i64::try_from(EXPIRES_IN_SECS).unwrap(),
        None,
        None,
    )
    .await
    .unwrap();

    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now: SYSTEM_TIME.clone(),
            admin_role: "mzadmin".to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            // Make the refresh window very large so it's easy to test.
            refresh_drop_factor: 1.0,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );
    let frontegg_user = "user@_.com";
    let frontegg_password = &format!("mzp_{client_id}{secret}");

    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "log_filter".to_string(),
            "mz_frontegg_auth=debug,info".to_string(),
        )
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 0);

    // Connect once.
    let pg_client = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(frontegg_password)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await
        .unwrap();

    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        frontegg_user
    );
    drop(pg_client);

    // We should have a single authentication request.
    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 1);

    // Wait for the token to expire.
    sleep(Duration::from_secs(EXPIRES_IN_SECS + 5)).await;

    // Check that our metrics indicate we're refreshing because of a recent drop.
    let metrics = server.metrics_registry.gather();
    let mut metrics: Vec<_> = metrics
        .into_iter()
        .filter(|family| family.name() == "mz_auth_session_refresh_count")
        .collect();
    assert_eq!(metrics.len(), 1);
    let metric = metrics.pop().unwrap();
    let metric = &metric.get_metric()[0];
    assert_eq!(metric.get_counter().get_value(), 1.0);

    let labels = metric.get_label();
    assert_eq!(
        (labels[0].name(), labels[0].value()),
        ("outstanding_receivers", "false")
    );
    assert_eq!(
        (labels[1].name(), labels[1].value()),
        ("recent_drop", "true")
    );

    // We should have automatically refreshed the token, even though there are not any active
    // handles!
    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 2);

    // Start a second session.
    let pg_client = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(frontegg_password)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await
        .unwrap();

    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        frontegg_user
    );

    // We should not have issued another auth request, because we should have attached to the
    // pre-emptively refreshed session.
    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 2);
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_refresh_dropped_session_lru() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let mut users: BTreeMap<String, UserConfig> = BTreeMap::new();

    let mut make_user = |email: &str| -> (Uuid, Uuid) {
        let password = Uuid::new_v4().to_string();
        let client_id = Uuid::new_v4();
        let secret = Uuid::new_v4();
        let created_at = Utc::now();

        let user = UserConfig {
            id: Uuid::new_v4(),
            email: email.to_string(),
            password,
            tenant_id,
            initial_api_tokens: vec![ApiToken {
                client_id,
                secret,
                description: None,
                created_at,
            }],
            roles: Vec::new(),
            auth_provider: None,
            verified: None,
            metadata: None,
        };
        users.insert(email.to_string(), user);

        (client_id, secret)
    };

    let user_a = "user_a@_.com";
    let (client_id_a, secret_a) = make_user(user_a);
    let password_a = &format!("mzp_{client_id_a}{secret_a}");

    let user_b = "user_b@_.com";
    let (client_id_b, secret_b) = make_user(user_b);
    let password_b = &format!("mzp_{client_id_b}{secret_b}");

    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();
    let now = SYSTEM_TIME.clone();

    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        now.clone(),
        i64::try_from(EXPIRES_IN_SECS).unwrap(),
        None,
        None,
    )
    .await
    .unwrap();

    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now: SYSTEM_TIME.clone(),
            admin_role: "mzadmin".to_string(),
            // Make the refresh LRU cache very small, and the window size very large so it's easy
            // to test.
            refresh_drop_lru_size: NonZeroUsize::new(1).expect("known non-zero"),
            refresh_drop_factor: 1.0,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );

    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "log_filter".to_string(),
            "mz_frontegg_auth=debug,info".to_string(),
        )
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 0);

    // Connect with user_a.
    let pg_client = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(user_a)
        .password(password_a)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await
        .unwrap();

    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        user_a
    );
    drop(pg_client);

    // Connect with user_b.
    let pg_client = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(user_b)
        .password(password_b)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await
        .unwrap();

    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        user_b
    );
    drop(pg_client);

    // We should have a single authentication request.
    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 2);

    // Wait for the token to expire.
    sleep(Duration::from_secs(EXPIRES_IN_SECS + 5)).await;

    // We should have refreshed one auth session, but not both.
    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 3);

    // We should be able to reconnect with user_b without incurring another auth request, its
    // session should have been refreshed.
    let pg_client = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(user_b)
        .password(password_b)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await
        .unwrap();

    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        user_b
    );
    // Re-connecting as `user_b` should not result in another auth request.
    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 3);
    drop(pg_client);

    let pg_client = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(user_a)
        .password(password_a)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await
        .unwrap();

    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        user_a
    );
    // Re-connecting with user_a should cause another auth request because we won't have an active
    // session, since its drop time was pushed out of the LRU.
    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 4);
    drop(pg_client);
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_transient_auth_failures() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let created_at = Utc::now();

    let frontegg_user = "user@_.com";

    let users = BTreeMap::from([(
        frontegg_user.to_string(),
        UserConfig {
            id: Uuid::new_v4(),
            email: frontegg_user.to_string(),
            password,
            tenant_id,
            initial_api_tokens: vec![ApiToken {
                client_id,
                secret,
                description: None,
                created_at,
            }],
            roles: Vec::new(),
            auth_provider: None,
            verified: None,
            metadata: None,
        },
    )]);
    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();
    let now = SYSTEM_TIME.clone();

    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        now.clone(),
        i64::try_from(EXPIRES_IN_SECS).unwrap(),
        None,
        None,
    )
    .await
    .unwrap();

    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now: SYSTEM_TIME.clone(),
            admin_role: "mzadmin".to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );
    let frontegg_user = "user@_.com";
    let frontegg_password = &format!("mzp_{client_id}{secret}");

    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "log_filter".to_string(),
            "mz_frontegg_auth=debug,info".to_string(),
        )
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    // Disable auth.
    frontegg_server.enable_auth.store(false, Ordering::Relaxed);

    // Try connecting once, we should fail.
    let result = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(frontegg_password)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await;
    assert_err!(result);

    // Re-enable auth.
    frontegg_server.enable_auth.store(true, Ordering::Relaxed);

    let pg_client = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(frontegg_password)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await
        .unwrap();

    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        frontegg_user
    );
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_transient_auth_failure_on_refresh() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let created_at = Utc::now();

    let frontegg_user = "user@_.com";

    let users = BTreeMap::from([(
        frontegg_user.to_string(),
        UserConfig {
            id: Uuid::new_v4(),
            email: frontegg_user.to_string(),
            password,
            tenant_id,
            initial_api_tokens: vec![ApiToken {
                client_id,
                secret,
                description: None,
                created_at,
            }],
            roles: Vec::new(),
            auth_provider: None,
            verified: None,
            metadata: None,
        },
    )]);
    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();
    let now = SYSTEM_TIME.clone();

    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        now.clone(),
        i64::try_from(EXPIRES_IN_SECS).unwrap(),
        None,
        None,
    )
    .await
    .unwrap();

    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now: SYSTEM_TIME.clone(),
            admin_role: "mzadmin".to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );
    let frontegg_user = "user@_.com";
    let frontegg_password = &format!("mzp_{client_id}{secret}");

    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "log_filter".to_string(),
            "mz_frontegg_auth=debug,info".to_string(),
        )
        .with_tls(server_cert, server_key)
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    // Connect once.
    let pg_client = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(frontegg_password)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await
        .unwrap();

    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        frontegg_user
    );
    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 1);

    // Disable auth.
    frontegg_server.enable_auth.store(false, Ordering::Relaxed);

    // Wait for refresh to occur.
    frontegg_server.wait_for_auth(EXPIRES_IN_SECS);
    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 2);

    // Our client should have been closed since auth refresh failed.
    assert_err!(pg_client.query_one("SELECT 1", &[]).await);

    // Re-enable auth.
    frontegg_server.enable_auth.store(true, Ordering::Relaxed);

    // We should be able to reconnect.
    let pg_client2 = server
        .connect()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(frontegg_password)
        .with_tls(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .await
        .unwrap();
    assert_ok!(pg_client2.query_one("SELECT 1", &[]).await);
    assert_eq!(*frontegg_server.auth_requests.lock().unwrap(), 3);
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_password_auth() {
    let metrics_registry = MetricsRegistry::new();

    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "log_filter".to_string(),
            "mz_frontegg_auth=debug,info".to_string(),
        )
        .with_system_parameter_default("enable_password_auth".to_string(), "true".to_string())
        .with_password_auth(Password("mz_system_password".to_owned()))
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    let mz_system_client = server
        .connect()
        .no_tls()
        .user("mz_system")
        .password("mz_system_password")
        .await
        .unwrap();

    mz_system_client
        .execute("ALTER SYSTEM SET scram_iterations to 9999", &[])
        .await
        .unwrap();

    mz_system_client
        .execute("CREATE ROLE foo WITH LOGIN PASSWORD 'bar'", &[])
        .await
        .unwrap();

    let external_client = server
        .connect()
        .no_tls()
        .user("foo")
        .password("bar")
        .await
        .unwrap();

    assert_eq!(
        external_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "foo"
    );

    assert_eq!(
        external_client
            .query_one("SELECT mz_is_superuser()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0),
        false
    );

    // Validate hash iterations.
    // change the iteratoins, ensure that login works
    // from the prior role and a new role
    mz_system_client
        .execute("ALTER SYSTEM SET scram_iterations to 9998", &[])
        .await
        .unwrap();

    mz_system_client
        .execute("CREATE ROLE foo_2 WITH LOGIN PASSWORD 'bar'", &[])
        .await
        .unwrap();

    server
        .connect()
        .no_tls()
        .user("foo")
        .password("bar")
        .await
        .unwrap();

    server
        .connect()
        .no_tls()
        .user("foo_2")
        .password("bar")
        .await
        .unwrap();
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_sasl_auth() {
    let metrics_registry = MetricsRegistry::new();

    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "log_filter".to_string(),
            "mz_frontegg_auth=debug,info".to_string(),
        )
        .with_system_parameter_default("enable_password_auth".to_string(), "true".to_string())
        .with_sasl_scram_auth(Password("mz_system_password".to_owned()))
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    let mz_system_client = server
        .connect()
        .no_tls()
        .user("mz_system")
        .password("mz_system_password")
        .await
        .unwrap();
    mz_system_client
        .execute("CREATE ROLE foo WITH LOGIN PASSWORD 'bar'", &[])
        .await
        .unwrap();

    let external_client = server
        .connect()
        .no_tls()
        .user("foo")
        .password("bar")
        .await
        .unwrap();

    assert_eq!(
        external_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "foo"
    );

    assert_eq!(
        external_client
            .query_one("SELECT mz_is_superuser()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0),
        false
    );
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_sasl_auth_failure() {
    let metrics_registry = MetricsRegistry::new();

    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "log_filter".to_string(),
            "mz_frontegg_auth=debug,info".to_string(),
        )
        .with_system_parameter_default("enable_password_auth".to_string(), "true".to_string())
        .with_sasl_scram_auth(Password("mz_system_password".to_owned()))
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    let mz_system_client = server
        .connect()
        .no_tls()
        .user("mz_system")
        .password("mz_system_password")
        .await
        .unwrap();
    mz_system_client
        .execute("CREATE ROLE foo WITH LOGIN PASSWORD 'bar'", &[])
        .await
        .unwrap();

    let external_client = server
        .connect()
        .no_tls()
        .user("foo")
        .password("wrong_password")
        .await;
    assert_err!(external_client);

    let external_client = server
        .connect()
        .no_tls()
        .user("no_user")
        .password("wrong_password")
        .await;

    assert_err!(external_client);
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_password_auth_superuser() {
    let metrics_registry = MetricsRegistry::new();

    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "log_filter".to_string(),
            "mz_frontegg_auth=debug,info".to_string(),
        )
        .with_system_parameter_default("enable_password_auth".to_string(), "true".to_string())
        .with_password_auth(Password("password".to_owned()))
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    let mz_system_client = server
        .connect()
        .no_tls()
        .user("mz_system")
        .password("password")
        .await
        .unwrap();
    mz_system_client
        .execute("CREATE ROLE foo WITH LOGIN SUPERUSER PASSWORD 'bar'", &[])
        .await
        .unwrap();

    let external_client = server
        .connect()
        .no_tls()
        .user("foo")
        .password("bar")
        .await
        .unwrap();

    assert_eq!(
        external_client
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "foo"
    );

    assert_eq!(
        external_client
            .query_one("SELECT mz_is_superuser()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0),
        true
    );
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_password_auth_alter_role() {
    let metrics_registry = MetricsRegistry::new();

    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "log_filter".to_string(),
            "mz_frontegg_auth=debug,info".to_string(),
        )
        .with_system_parameter_default("enable_password_auth".to_string(), "true".to_string())
        .with_password_auth(Password("mz_system_password".to_owned()))
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    let mz_system_client = server
        .connect()
        .no_tls()
        .user("mz_system")
        .password("mz_system_password")
        .await
        .unwrap();
    mz_system_client
        .execute("CREATE ROLE foo WITH LOGIN PASSWORD 'bar'", &[])
        .await
        .unwrap();

    {
        let external_client = server
            .connect()
            .no_tls()
            .user("foo")
            .password("bar")
            .await
            .unwrap();

        assert_eq!(
            external_client
                .query_one("SELECT current_user", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "foo"
        );

        assert_eq!(
            external_client
                .query_one("SELECT mz_is_superuser()", &[])
                .await
                .unwrap()
                .get::<_, bool>(0),
            false
        );
    }

    mz_system_client
        .execute("ALTER ROLE foo WITH SUPERUSER PASSWORD 'baz'", &[])
        .await
        .unwrap();

    {
        let external_client = server
            .connect()
            .no_tls()
            .user("foo")
            .password("baz")
            .await
            .unwrap();

        assert_eq!(
            external_client
                .query_one("SELECT current_user", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "foo"
        );

        assert_eq!(
            external_client
                .query_one("SELECT mz_is_superuser()", &[])
                .await
                .unwrap()
                .get::<_, bool>(0),
            true
        );
    }

    mz_system_client
        .execute("ALTER ROLE foo WITH SUPERUSER PASSWORD NULL", &[])
        .await
        .unwrap();

    {
        assert_err!(server.connect().no_tls().user("foo").password("baz").await);
    }

    mz_system_client
        .execute("ALTER ROLE foo WITH SUPERUSER PASSWORD 'baz'", &[])
        .await
        .unwrap();

    {
        let external_client = server
            .connect()
            .no_tls()
            .user("foo")
            .password("baz")
            .await
            .unwrap();

        assert_eq!(
            external_client
                .query_one("SELECT current_user", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "foo"
        );

        assert_eq!(
            external_client
                .query_one("SELECT mz_is_superuser()", &[])
                .await
                .unwrap()
                .get::<_, bool>(0),
            true
        );
    }
    mz_system_client
        .execute("ALTER ROLE foo WITH NOLOGIN", &[])
        .await
        .unwrap();

    {
        assert_err!(server.connect().no_tls().user("foo").password("baz").await);
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_password_auth_http() {
    let metrics_registry = MetricsRegistry::new();

    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "log_filter".to_string(),
            "mz_frontegg_auth=debug,info".to_string(),
        )
        .with_system_parameter_default("enable_password_auth".to_string(), "true".to_string())
        .with_password_auth(Password("mz_system_password".to_owned()))
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    let ws_url: Uri = format!("ws://{}/api/experimental/sql", server.http_local_addr())
        .parse()
        .unwrap();
    let login_url: Uri = format!("http://{}/api/login", server.http_local_addr())
        .parse()
        .unwrap();
    let http_url: Uri = format!("http://{}/api/sql", server.http_local_addr())
        .parse()
        .unwrap();

    let query = r#"{"query":"SELECT current_user"}"#;
    let ws_options_msg = Message::Text(r#"{"options": {}}"#.to_owned().into());

    let http_client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(10))
        .build_http();

    // Should fail due to not being logged in.
    assert_eq!(
        http_client
            .request(
                Request::post(http_url.clone())
                    .header("Content-Type", "application/json",)
                    .body(query.to_owned())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );

    // Should fail due to not being logged in.
    let ws_request = ClientRequestBuilder::new(ws_url.clone());
    let (mut ws, _resp) = tungstenite::connect(ws_request).unwrap();
    ws.send(ws_options_msg.clone()).unwrap();
    assert_eq!(
        ws.read().unwrap(),
        Message::Close(Some(CloseFrame {
            code: CloseCode::Protocol,
            reason: Utf8Bytes::from_static("unauthorized")
        })),
    );

    // Login. Subsequent requests should be work.
    let login_response = http_client
        .request(
            Request::post(login_url)
                .header("Content-Type", "application/json")
                .body(r#"{"username":"mz_system","password":"mz_system_password"}"#.to_owned())
                .unwrap(),
        )
        .await
        .unwrap();

    // We need to manually add the cookie header to the tungstenite request.
    let session_cookie = login_response
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split("; ")
        .find(|v| v.starts_with("mz_session="))
        .unwrap();

    #[derive(Deserialize)]
    struct Result {
        rows: Vec<Vec<String>>,
    }
    #[derive(Deserialize)]
    struct Response {
        results: Vec<Result>,
    }
    let body = http_client
        .request(
            Request::post(http_url)
                .header("Content-Type", "application/json")
                .header(COOKIE, session_cookie)
                .body(query.to_owned())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(
        serde_json::from_slice::<Response>(&body).unwrap().results[0].rows[0][0],
        "mz_system"
    );

    let ws_request = ClientRequestBuilder::new(ws_url).with_header("Cookie", session_cookie);
    let (mut ws, _resp) = tungstenite::connect(ws_request).unwrap();
    ws.send(ws_options_msg.clone()).unwrap();
    // Websockets send a bunch of unrelated stuff before getting to the query and rows
    let mut messages = Vec::with_capacity(100);
    loop {
        let resp = ws.read().unwrap();
        match resp {
            Message::Text(msg) => {
                let msg: WebSocketResponse = serde_json::from_str(&msg).unwrap();
                match msg {
                    WebSocketResponse::ReadyForQuery(_) => {
                        ws.send(Message::Text(query.to_owned().into())).unwrap();
                    }
                    WebSocketResponse::Row(rows) => {
                        assert_eq!(&rows, &[serde_json::Value::from("mz_system".to_owned())]);
                        break;
                    }
                    _ => {
                        messages.push(msg);
                        if messages.len() >= 100 {
                            panic!("giving up after many unexpected messages: {:#?}", messages);
                        }
                    }
                }
            }
            Message::Ping(_) => continue,
            _ => panic!("unexpected response: {:?}", resp),
        }
    }
}

/// Tests that the superuser flag is correctly propagated through HTTP/WebSocket authentication.
/// This is a regression test for a bug where WebSocket connections always had superuser=false
/// because internal_user_metadata was hardcoded to None.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl` on OS `linux`
async fn test_password_auth_http_superuser() {
    let metrics_registry = MetricsRegistry::new();

    let server = test_util::TestHarness::default()
        .with_system_parameter_default(
            "log_filter".to_string(),
            "mz_frontegg_auth=debug,info".to_string(),
        )
        .with_system_parameter_default("enable_password_auth".to_string(), "true".to_string())
        .with_password_auth(Password("mz_system_password".to_owned()))
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    let mz_system_client = server
        .connect()
        .no_tls()
        .user("mz_system")
        .password("mz_system_password")
        .await
        .unwrap();
    mz_system_client
        .execute(
            "CREATE ROLE superuser_role WITH LOGIN SUPERUSER PASSWORD 'super_pass'",
            &[],
        )
        .await
        .unwrap();
    mz_system_client
        .execute(
            "CREATE ROLE normal_role WITH LOGIN PASSWORD 'normal_pass'",
            &[],
        )
        .await
        .unwrap();

    let ws_url: Uri = format!("ws://{}/api/experimental/sql", server.http_local_addr())
        .parse()
        .unwrap();
    let login_url: Uri = format!("http://{}/api/login", server.http_local_addr())
        .parse()
        .unwrap();

    let http_client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(10))
        .build_http();

    fn check_superuser_via_ws_basic(ws_url: &Uri, user: &str, password: &str) -> bool {
        let ws_request = ClientRequestBuilder::new(ws_url.clone());
        let (mut ws, _resp) = tungstenite::connect(ws_request).unwrap();

        let auth = WebSocketAuth::Basic {
            user: user.to_string(),
            password: Password(password.to_string()),
            options: BTreeMap::default(),
        };
        ws.send(Message::Text(serde_json::to_string(&auth).unwrap().into()))
            .unwrap();

        loop {
            let resp = ws.read().unwrap();
            if let Message::Text(msg) = resp {
                let msg: WebSocketResponse = serde_json::from_str(&msg).unwrap();
                if matches!(msg, WebSocketResponse::ReadyForQuery(_)) {
                    break;
                }
            }
        }

        ws.send(Message::Text(
            r#"{"query": "SHOW is_superuser"}"#.to_owned().into(),
        ))
        .unwrap();

        loop {
            let resp = ws.read().unwrap();
            if let Message::Text(msg) = resp {
                let msg: WebSocketResponse = serde_json::from_str(&msg).unwrap();
                if let WebSocketResponse::Row(row) = msg {
                    let value = row[0].as_str().unwrap();
                    return value == "on";
                }
            }
        }
    }

    async fn check_superuser_via_ws_session(
        http_client: &hyper_util::client::legacy::Client<
            hyper_util::client::legacy::connect::HttpConnector,
            String,
        >,
        login_url: &Uri,
        ws_url: &Uri,
        user: &str,
        password: &str,
    ) -> bool {
        let login_body = format!(r#"{{"username":"{}","password":"{}"}}"#, user, password);
        let login_response = http_client
            .request(
                Request::post(login_url.clone())
                    .header("Content-Type", "application/json")
                    .body(login_body)
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(login_response.status(), StatusCode::OK);

        let session_cookie = login_response
            .headers()
            .get(SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .split("; ")
            .find(|v| v.starts_with("mz_session="))
            .unwrap();

        let ws_request =
            ClientRequestBuilder::new(ws_url.clone()).with_header("Cookie", session_cookie);
        let (mut ws, _resp) = tungstenite::connect(ws_request).unwrap();

        ws.send(Message::Text(r#"{"options": {}}"#.to_owned().into()))
            .unwrap();

        loop {
            let resp = ws.read().unwrap();
            if let Message::Text(msg) = resp {
                let msg: WebSocketResponse = serde_json::from_str(&msg).unwrap();
                if matches!(msg, WebSocketResponse::ReadyForQuery(_)) {
                    break;
                }
            }
        }

        ws.send(Message::Text(
            r#"{"query": "SHOW is_superuser"}"#.to_owned().into(),
        ))
        .unwrap();

        loop {
            let resp = ws.read().unwrap();
            if let Message::Text(msg) = resp {
                let msg: WebSocketResponse = serde_json::from_str(&msg).unwrap();
                if let WebSocketResponse::Row(row) = msg {
                    let value = row[0].as_str().unwrap();
                    return value == "on";
                }
            }
        }
    }

    // Superuser via WebSocket with Basic auth should have is_superuser=on
    assert!(
        check_superuser_via_ws_basic(&ws_url, "superuser_role", "super_pass"),
        "superuser_role should have is_superuser=on via WebSocket Basic auth"
    );

    // Non-superuser via WebSocket with Basic auth should have is_superuser=off
    assert!(
        !check_superuser_via_ws_basic(&ws_url, "normal_role", "normal_pass"),
        "normal_role should have is_superuser=off via WebSocket Basic auth"
    );

    // Superuser via WebSocket with session cookie should have is_superuser=on
    assert!(
        check_superuser_via_ws_session(
            &http_client,
            &login_url,
            &ws_url,
            "superuser_role",
            "super_pass"
        )
        .await,
        "superuser_role should have is_superuser=on via WebSocket session auth"
    );

    // Non-superuser via WebSocket with session cookie should have is_superuser=off
    assert!(
        !check_superuser_via_ws_session(
            &http_client,
            &login_url,
            &ws_url,
            "normal_role",
            "normal_pass"
        )
        .await,
        "normal_role should have is_superuser=off via WebSocket session auth"
    );

    // mz_system (internal user) via Basic auth should have is_superuser=on
    assert!(
        check_superuser_via_ws_basic(&ws_url, "mz_system", "mz_system_password"),
        "mz_system should have is_superuser=on via WebSocket Basic auth"
    );
}
