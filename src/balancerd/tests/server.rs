// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for balancerd.

#![recursion_limit = "256"]

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use chrono::Utc;
use futures::StreamExt;
use jsonwebtoken::{DecodingKey, EncodingKey};
use mz_balancerd::{
    BUILD_INFO, BalancerConfig, BalancerResolver, BalancerService, CancellationResolver,
    FronteggResolver, SniTemplate, TenantDnsResolver,
};
use mz_environmentd::test_util::{self, Ca, make_pg_tls};
use mz_frontegg_auth::{
    Authenticator as FronteggAuthentication, AuthenticatorConfig as FronteggConfig,
    DEFAULT_REFRESH_DROP_FACTOR, DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
};
use mz_frontegg_mock::{FronteggMockServer, models::ApiToken, models::UserConfig};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::id_gen::{conn_id_org_uuid, org_id_conn_bits};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::netio::MAX_FRAME_SIZE;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::retry::Retry;
use mz_ore::tracing::TracingHandle;
use mz_ore::{assert_contains, assert_err, assert_ok, task};
use mz_pgwire_common::{
    ACCEPT_SSL_ENCRYPTION, FrontendStartupMessage, MAX_FORWARDED_STARTUP_FRAME_SIZE,
    MAX_STARTUP_FRAME_SIZE, REJECT_ENCRYPTION, VERSION_3,
};
use mz_server_core::TlsCertConfig;
use openssl::ssl::{SslConnector, SslConnectorBuilder, SslMethod, SslVerifyMode};
use openssl::x509::X509;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use uuid::Uuid;

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
#[allow(clippy::disallowed_methods)]
async fn test_balancer() {
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

    const EXPIRES_IN_SECS: i64 = 50;
    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        SYSTEM_TIME.clone(),
        EXPIRES_IN_SECS,
        // Add a bit of delay so we can test connection de-duplication.
        Some(Duration::from_millis(100)),
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
    let frontegg_password = format!("mzp_{client_id}{secret}");

    let config = test_util::TestHarness::default()
        // Enable SSL on the main port. There should be a balancerd port with no SSL.
        .with_tls(server_cert.clone(), server_key.clone())
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry);
    let envid = config.environment_id.clone();
    let envd_server = config.start().await;

    let cancel_dir = tempfile::tempdir().unwrap();
    let cancel_name = conn_id_org_uuid(org_id_conn_bits(&envid.organization_id()));
    std::fs::write(
        cancel_dir.path().join(cancel_name),
        format!(
            "{}\n{}",
            envd_server.sql_local_addr(),
            // Ensure that multiline files and non-existent addresses both work.
            "non-existent-addr:1234",
        ),
    )
    .unwrap();

    let resolvers = vec![
        (
            BalancerResolver::Static(envd_server.sql_local_addr().to_string()),
            CancellationResolver::Static(envd_server.sql_local_addr().to_string()),
        ),
        (
            BalancerResolver::MultiTenant {
                dns: Arc::new(
                    TenantDnsResolver::new().expect("system DNS configuration is readable"),
                ),
                frontegg: FronteggResolver {
                    auth: frontegg_auth,
                    addr_template: envd_server.sql_local_addr().to_string(),
                },
                sni: Some(SniTemplate {
                    template: envd_server.sql_local_addr().ip().to_string(),
                    port: envd_server.sql_local_addr().port(),
                }),
            },
            CancellationResolver::Directory(cancel_dir.path().to_owned()),
        ),
    ];
    let cert_config = Some(TlsCertConfig {
        cert: server_cert.clone(),
        key: server_key.clone(),
    });

    let body = r#"{"query": "select 12234"}"#;
    let ca_cert = reqwest::Certificate::from_pem(&ca.cert.to_pem().unwrap()).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(ca_cert)
        // No pool so that connections are never re-used which can use old ssl certs.
        .pool_max_idle_per_host(0)
        .tls_info(true)
        .build()
        .unwrap();

    for (resolver, cancellation_resolver) in resolvers {
        let (mut reload_tx, reload_rx) = futures::channel::mpsc::channel(1);
        let ticker = Box::pin(reload_rx);
        let is_multi_tenant_resolver = matches!(resolver, BalancerResolver::MultiTenant { .. });
        let balancer_cfg = BalancerConfig::new(
            &BUILD_INFO,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            cancellation_resolver,
            resolver,
            envd_server.http_local_addr().to_string(),
            cert_config.clone(),
            true,
            MetricsRegistry::new(),
            ticker,
            None,
            None,
            Duration::ZERO,
            None,
            None,
            None,
            TracingHandle::disabled(),
            // Advertise HTTP/2 via ALPN. This defaults off in production so a
            // balancerd that upgrades ahead of environmentd does not offer h2
            // to clients before environmentd can parse it.
            vec![(
                "balancerd_https_enable_http2_alpn".to_string(),
                "true".to_string(),
            )],
        );
        let balancer_server = BalancerService::new(balancer_cfg).await.unwrap();
        let balancer_pgwire_listen = balancer_server.pgwire.0.local_addr();
        let balancer_https_listen = balancer_server.https.0.local_addr();
        let balancer_https_internal = balancer_server.internal_http.0.local_addr();
        task::spawn(|| "balancer", async {
            balancer_server.serve().await.unwrap();
        });

        let conn_str = Arc::new(format!(
            "user={frontegg_user} password={frontegg_password} host={} port={} sslmode=require",
            balancer_pgwire_listen.ip(),
            balancer_pgwire_listen.port()
        ));

        let tls = make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        }));

        let (pg_client, conn) = tokio_postgres::connect(&conn_str, tls.clone())
            .await
            .unwrap();
        task::spawn(|| "balancer-pg_client", async move {
            let _ = conn.await;
        });

        let res: i32 = pg_client.query_one("SELECT 2", &[]).await.unwrap().get(0);
        assert_eq!(res, 2);

        // A wrong password on the Frontegg (multi-tenant) path must fail with
        // SQLSTATE 28P01 and the exact opaque message "invalid password", with
        // no internal detail leaked to the client.
        if is_multi_tenant_resolver {
            let wrong_password = format!("mzp_{}{}", Uuid::new_v4(), Uuid::new_v4());
            let bad_conn_str = format!(
                "user={frontegg_user} password={wrong_password} host={} port={} sslmode=require",
                balancer_pgwire_listen.ip(),
                balancer_pgwire_listen.port()
            );
            let err = match tokio_postgres::connect(&bad_conn_str, tls.clone()).await {
                Ok(_) => panic!("connection with wrong password should have failed"),
                Err(e) => e,
            };
            let db_err = err
                .as_db_error()
                .expect("expected a database error from the server");
            assert_eq!(
                db_err.code(),
                &tokio_postgres::error::SqlState::INVALID_PASSWORD
            );
            assert_eq!(db_err.message(), "invalid password");
        }

        // Assert cancellation is propagated.
        let cancel = pg_client.cancel_token();
        let copy = pg_client
            .copy_out("copy (subscribe (select * from mz_kafka_sinks)) to stdout")
            .await
            .unwrap();
        let _ = cancel.cancel_query(tls).await;
        let e = pin!(copy).next().await.unwrap().unwrap_err();
        assert_contains!(
            e.to_string_with_causes(),
            "canceling statement due to user request"
        );

        // Various tests about reloading of certs.

        // Assert the current certificate is as expected.
        let https_url = format!(
            "https://{host}:{port}/api/sql",
            host = balancer_https_listen.ip(),
            port = balancer_https_listen.port()
        );
        let resp = client
            .post(&https_url)
            .header("Content-Type", "application/json")
            .basic_auth(frontegg_user, Some(&frontegg_password))
            .body(body)
            .send()
            .await
            .unwrap();
        let tlsinfo = resp.extensions().get::<reqwest::tls::TlsInfo>().unwrap();
        let resp_x509 = X509::from_der(tlsinfo.peer_certificate().unwrap()).unwrap();
        let server_x509 = X509::from_pem(&std::fs::read(&server_cert).unwrap()).unwrap();
        assert_eq!(resp_x509, server_x509);
        assert_eq!(resp.version(), reqwest::Version::HTTP_11);
        assert_contains!(resp.text().await.unwrap(), "12234");

        // With `balancerd_https_enable_http2_alpn` set, balancerd offers h2 to
        // clients that ask for it. reqwest's native-tls backend does not, hence
        // the HTTP/1.1 responses either side of this.
        assert_eq!(
            alpn_selected(balancer_https_listen, b"\x02h2\x08http/1.1")
                .await
                .as_deref(),
            Some(&b"h2"[..])
        );
        assert_eq!(
            alpn_selected(balancer_https_listen, b"\x08http/1.1")
                .await
                .as_deref(),
            Some(&b"http/1.1"[..])
        );

        // HTTP/1.1-only clients are still served.
        let http1_client = reqwest::Client::builder()
            .add_root_certificate(
                reqwest::Certificate::from_pem(&ca.cert.to_pem().unwrap()).unwrap(),
            )
            .pool_max_idle_per_host(0)
            .http1_only()
            .build()
            .unwrap();
        let resp = http1_client
            .post(&https_url)
            .header("Content-Type", "application/json")
            .basic_auth(frontegg_user, Some(&frontegg_password))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.version(), reqwest::Version::HTTP_11);
        assert_contains!(resp.text().await.unwrap(), "12234");

        // Generate new certs. Install only the key, reload, and make sure the old cert is still in
        // use.
        let (next_cert, next_key) = ca
            .request_cert("next", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .unwrap();
        let next_x509 = X509::from_pem(&std::fs::read(&next_cert).unwrap()).unwrap();
        assert_ne!(next_x509, server_x509);
        std::fs::copy(next_key, &server_key).unwrap();
        let (tx, rx) = oneshot::channel();
        reload_tx.try_send(Some(tx)).unwrap();
        let res = rx.await.unwrap();
        assert_err!(res);

        // We should still be on the old cert because now the cert and key mismatch.
        let resp = client
            .post(&https_url)
            .header("Content-Type", "application/json")
            .basic_auth(frontegg_user, Some(&frontegg_password))
            .body(body)
            .send()
            .await
            .unwrap();
        let tlsinfo = resp.extensions().get::<reqwest::tls::TlsInfo>().unwrap();
        let resp_x509 = X509::from_der(tlsinfo.peer_certificate().unwrap()).unwrap();
        assert_eq!(resp_x509, server_x509);

        // Now move the cert too. Reloading should succeed and the response should have the new
        // cert.
        std::fs::copy(next_cert, &server_cert).unwrap();
        let (tx, rx) = oneshot::channel();
        reload_tx.try_send(Some(tx)).unwrap();
        let res = rx.await.unwrap();
        assert_ok!(res);
        let resp = client
            .post(&https_url)
            .header("Content-Type", "application/json")
            .basic_auth(frontegg_user, Some(&frontegg_password))
            .body(body)
            .send()
            .await
            .unwrap();
        let tlsinfo = resp.extensions().get::<reqwest::tls::TlsInfo>().unwrap();
        let resp_x509 = X509::from_der(tlsinfo.peer_certificate().unwrap()).unwrap();
        assert_eq!(resp_x509, next_x509);

        if !is_multi_tenant_resolver {
            continue;
        }

        // Test de-duplication in the frontegg resolver. This is a bit racy so use a retry loop.
        Retry::default()
            .max_duration(Duration::from_secs(30))
            .retry_async(|_| async {
                let start_auth_count = *frontegg_server.auth_requests.lock().unwrap();
                const CONNS: u64 = 10;
                let mut handles = Vec::with_capacity(usize::cast_from(CONNS));
                for _ in 0..CONNS {
                    let conn_str = Arc::clone(&conn_str);
                    let handle = task::spawn(|| "test conn", async move {
                        let (pg_client, conn) = tokio_postgres::connect(
                            &conn_str,
                            make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
                                Ok(b.set_verify(SslVerifyMode::NONE))
                            })),
                        )
                        .await
                        .unwrap();
                        task::spawn(|| "balancer-pg_client", async move {
                            let _ = conn.await;
                        });
                        let res: i32 = pg_client.query_one("SELECT 2", &[]).await.unwrap().get(0);
                        assert_eq!(res, 2);
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.await;
                }
                let end_auth_count = *frontegg_server.auth_requests.lock().unwrap();
                // We expect that the auth count increased by fewer than the number of connections.
                if end_auth_count == start_auth_count + CONNS {
                    // No deduplication was done, try again.
                    return Err("no auth dedup");
                }
                Ok(())
            })
            .await
            .unwrap();

        // Assert some metrics are being tracked.
        let metrics_url = format!(
            "http://{host}:{port}/metrics",
            host = balancer_https_internal.ip(),
            port = balancer_https_internal.port()
        );
        Retry::default()
            .max_duration(Duration::from_secs(30))
            .retry_async(|_| async {
                let resp = client
                    .get(&metrics_url)
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap();
                if !resp.contains("mz_balancer_tenant_connection_active") {
                    return Err("mz_balancer_tenant_connection_active");
                }
                if !resp.contains("mz_balancer_tenant_connection_rx") {
                    return Err("mz_balancer_tenant_connection_rx");
                }
                Ok(())
            })
            .await
            .unwrap();

        // The internal HTTP server serves h2c (HTTP/2 with prior knowledge)
        // alongside HTTP/1.1.
        let h2c_client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .unwrap();
        let resp = h2c_client.get(&metrics_url).send().await.unwrap();
        assert_eq!(resp.version(), reqwest::Version::HTTP_2);
        assert!(resp.status().is_success());
    }
}

/// Starts a balancerd whose pgwire listener is reachable but whose upstream is
/// not. These tests never get far enough to be forwarded anywhere.
async fn start_balancer() -> SocketAddr {
    // Unreachable upstream: these tests never get far enough to be forwarded.
    start_balancer_to("127.0.0.1:1".to_string(), vec![], None)
        .await
        .pgwire
}

/// Listen addresses of a started balancerd.
struct Balancer {
    pgwire: SocketAddr,
    https: SocketAddr,
    internal_http: SocketAddr,
}

/// Starts a balancerd proxying to `upstream`, with the given dyncfg defaults.
async fn start_balancer_to(
    upstream: String,
    default_configs: Vec<(String, String)>,
    tls: Option<TlsCertConfig>,
) -> Balancer {
    let unreachable = upstream;
    let (_reload_tx, reload_rx) = futures::channel::mpsc::channel(1);
    let balancer_cfg = BalancerConfig::new(
        &BUILD_INFO,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        CancellationResolver::Static(unreachable.clone()),
        BalancerResolver::Static(unreachable.clone()),
        unreachable.clone(),
        tls,
        false,
        MetricsRegistry::new(),
        Box::pin(reload_rx),
        None,
        None,
        Duration::ZERO,
        None,
        None,
        None,
        TracingHandle::disabled(),
        default_configs,
    );
    let balancer_server = BalancerService::new(balancer_cfg).await.unwrap();
    let addrs = Balancer {
        pgwire: balancer_server.pgwire.0.local_addr(),
        https: balancer_server.https.0.local_addr(),
        internal_http: balancer_server.internal_http.0.local_addr(),
    };
    task::spawn(|| "balancer", async {
        balancer_server.serve().await.unwrap();
    });
    addrs
}

/// Narrows a metric to the pgwire listener's series.
const PGWIRE: Option<&str> = Some("source=\"pgwire\"");

/// The current value of a metric, optionally narrowed to lines carrying `label`.
async fn metric_value(internal_http: SocketAddr, name: &str, label: Option<&str>) -> Option<f64> {
    let body = reqwest::get(format!("http://{internal_http}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    body.lines()
        .find(|line| line.starts_with(name) && label.is_none_or(|l| line.contains(l)))
        .and_then(|line| line.rsplit(' ').next())
        .map(|value| value.parse().unwrap())
}

/// Waits for `name` to read `expected`. The server-side guard is dropped shortly after the
/// client observes the close, so the metric can lag the assertion by a moment.
async fn assert_metric(internal_http: SocketAddr, name: &str, label: Option<&str>, expected: f64) {
    Retry::default()
        .max_duration(Duration::from_secs(10))
        .retry_async(|_| async {
            match metric_value(internal_http, name, label).await {
                Some(value) if value == expected => Ok(()),
                other => Err(format!("{name} is {other:?}, expected {expected}")),
            }
        })
        .await
        .unwrap();
}

/// Opens a pgwire connection, writes a startup frame-length header declaring
/// `frame_len` bytes, and sends nothing further.
async fn startup_header_only(addr: SocketAddr, frame_len: u32) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(&frame_len.to_be_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    stream
}

/// Whether balancerd closes the connection within the specified duration.
async fn was_closed(stream: &mut TcpStream, within: Duration) -> bool {
    let mut byte = [0u8; 1];
    match tokio::time::timeout(within, stream.read(&mut byte)).await {
        Err(_elapsed) => false,
        Ok(Ok(0)) => true,
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => true,
        Ok(Ok(n)) => panic!("balancerd sent {n} bytes instead of closing or waiting: {byte:?}"),
        Ok(Err(e)) => panic!("unexpected error reading from balancerd: {e}"),
    }
}

/// A startup frame larger than the budget is refused on the raw socket, while
/// one at the budget is still served.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_pgwire_oversized_startup_frame_is_rejected() {
    let pgwire_addr = start_balancer().await;
    let budget = u32::try_from(MAX_STARTUP_FRAME_SIZE).expect("fits in a frame-length field");
    let protocol_max = u32::try_from(MAX_FRAME_SIZE).expect("fits in a frame-length field");

    for declared in [budget + 1, protocol_max] {
        let mut stream = startup_header_only(pgwire_addr, declared).await;
        assert!(
            was_closed(&mut stream, Duration::from_secs(10)).await,
            "balancerd accepted a {declared} byte startup frame and waited for the body",
        );
    }

    // The boundary itself is still served, so the rejection is the budget doing
    // its job rather than balancerd refusing everything.
    let mut stream = startup_header_only(pgwire_addr, budget).await;
    assert!(
        !was_closed(&mut stream, Duration::from_secs(10)).await,
        "balancerd rejected a startup frame at the budget",
    );

    // A well-formed client is still answered, which also rules out the checks
    // above passing against a listener that never came up: the socket is bound
    // in `BalancerService::new`, before `serve` runs its accept loop, so a
    // connection merely sitting in the accept backlog would read as "waiting".
    let mut probe = TcpStream::connect(pgwire_addr).await.unwrap();
    let mut ssl_request = BytesMut::new();
    FrontendStartupMessage::SslRequest
        .encode(&mut ssl_request)
        .unwrap();
    probe.write_all(&ssl_request).await.unwrap();
    let mut reply = [0u8; 1];
    probe.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [REJECT_ENCRYPTION]);
}

/// The startup frame a balancer forwards is larger than the one the client sent, because it
/// appends its own parameters. This measures what balancerd actually puts on the wire, rather
/// than trusting a hand-maintained list of which parameters those are, so adding a third one
/// fails here instead of silently overrunning the budget environmentd allows.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_forwarded_startup_frame_fits_downstream_budget() {
    // Stands in for environmentd, only to capture the frame balancerd sends it.
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    let pgwire_addr = start_balancer_to(upstream_addr.to_string(), vec![], None)
        .await
        .pgwire;

    // The largest startup frame balancerd will accept from a client. `user` is required to get
    // past `run`, and `options` pads the rest out to exactly the budget.
    let overhead = 4 + 4 + "user".len() + 1 + "mz".len() + 1 + "options".len() + 1 + 1 + 1;
    let params = BTreeMap::from([
        ("user".to_string(), "mz".to_string()),
        (
            "options".to_string(),
            "x".repeat(MAX_STARTUP_FRAME_SIZE - overhead),
        ),
    ]);
    let mut frame = BytesMut::new();
    FrontendStartupMessage::Startup {
        version: VERSION_3,
        params,
    }
    .encode(&mut frame)
    .unwrap();
    assert_eq!(frame.len(), MAX_STARTUP_FRAME_SIZE);

    let mut client = TcpStream::connect(pgwire_addr).await.unwrap();
    client.write_all(&frame).await.unwrap();
    client.flush().await.unwrap();

    let (mut forwarded_to, _) = tokio::time::timeout(Duration::from_secs(30), upstream.accept())
        .await
        .expect("balancerd should forward the connection upstream")
        .unwrap();
    let mut len = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(30), forwarded_to.read_exact(&mut len))
        .await
        .expect("balancerd should send a startup frame upstream")
        .unwrap();
    let forwarded_len = usize::cast_from(u32::from_be_bytes(len));

    assert!(
        forwarded_len <= MAX_FORWARDED_STARTUP_FRAME_SIZE,
        "balancerd forwarded a {forwarded_len} byte startup frame, over the \
         {MAX_FORWARDED_STARTUP_FRAME_SIZE} byte budget downstream allows. If a parameter was \
         added to the forwarded set, raise FORWARDED_STARTUP_PARAM_ALLOWANCE to match.",
    );
}

/// The connection limit counts a connection from the moment it is accepted, and the pre-resolved
/// deadline releases connections that never resolve, so the limit cannot be held shut by clients
/// that connect and say nothing. The metrics tell the two phases apart.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_connection_limit_covers_unresolved_connections() {
    const ACTIVE: &str = "mz_balancer_pre_resolved_connection_active";
    const TIMEOUTS: &str = "mz_balancer_pre_resolved_timeout_total";
    const REJECTED: &str = "mz_balancer_connection_rejected_total";
    let balancer = start_balancer_to(
        "127.0.0.1:1".to_string(),
        vec![
            ("balancerd_max_connections".into(), "1".into()),
            ("balancerd_pre_resolved_timeout".into(), "5s".into()),
        ],
        None,
    )
    .await;

    // One connection parks before resolving and holds the only slot.
    let mut held = startup_header_only(balancer.pgwire, 1 << 10).await;
    assert!(
        !was_closed(&mut held, Duration::from_millis(500)).await,
        "the first connection should be admitted and waited on",
    );
    assert_metric(balancer.internal_http, ACTIVE, PGWIRE, 1.0).await;

    // The next is refused at accept while the limit is reached. The window is well inside the
    // deadline on purpose: a wider one would also be satisfied by a connection that was wrongly
    // admitted and then closed by its own deadline, so the test would pass with no limit at all.
    let mut refused = startup_header_only(balancer.pgwire, 1 << 10).await;
    assert!(
        was_closed(&mut refused, Duration::from_secs(2)).await,
        "a connection beyond the limit should be refused at accept, not left to the deadline",
    );
    assert_metric(balancer.internal_http, REJECTED, None, 1.0).await;

    // The deadline reclaims the slot, so the limit is not a one-way door.
    assert!(
        was_closed(&mut held, Duration::from_secs(20)).await,
        "the held connection should be closed once the pre-resolved deadline passes",
    );
    assert_metric(balancer.internal_http, ACTIVE, PGWIRE, 0.0).await;
    assert_metric(balancer.internal_http, TIMEOUTS, PGWIRE, 1.0).await;
    let mut after = startup_header_only(balancer.pgwire, 1 << 10).await;
    assert!(
        !was_closed(&mut after, Duration::from_millis(500)).await,
        "capacity should be available again once the deadline reclaimed the slot",
    );
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_stalled_tls_handshake_is_closed() {
    const TIMEOUTS: &str = "mz_balancer_pre_resolved_timeout_total";
    let ca = Ca::new_root("test ca").unwrap();
    let (cert, key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let balancer = start_balancer_to(
        "127.0.0.1:1".to_string(),
        vec![("balancerd_pre_resolved_timeout".into(), "5s".into())],
        Some(TlsCertConfig { cert, key }),
    )
    .await;

    let mut stream = TcpStream::connect(balancer.pgwire).await.unwrap();
    let mut https_stream = TcpStream::connect(balancer.https).await.unwrap();
    let mut ssl_request = BytesMut::new();
    FrontendStartupMessage::SslRequest
        .encode(&mut ssl_request)
        .unwrap();
    stream.write_all(&ssl_request).await.unwrap();

    // balancerd agrees to TLS and waits for a ClientHello that never comes.
    let mut reply = [0u8; 1];
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [ACCEPT_SSL_ENCRYPTION]);

    assert!(
        was_closed(&mut stream, Duration::from_secs(20)).await,
        "a connection stalled mid-handshake should be closed once the deadline passes",
    );
    assert_metric(balancer.internal_http, TIMEOUTS, PGWIRE, 1.0).await;
    assert!(
        was_closed(&mut https_stream, Duration::from_secs(20)).await,
        "an HTTPS connection stalled mid-handshake should be closed once the deadline passes",
    );
    assert_metric(
        balancer.internal_http,
        TIMEOUTS,
        Some("source=\"https\""),
        1.0,
    )
    .await;
}

/// A client that is rejected during startup is told why, rather than having the connection
/// closed under it. `FramedConn::send` only enqueues, so these paths are only correct if the
/// rejection is flushed before the connection is dropped.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_startup_rejection_reaches_the_client() {
    let pgwire_addr = start_balancer().await;

    // An unsupported protocol version, which balancerd answers and then stops on.
    let mut frame = BytesMut::new();
    FrontendStartupMessage::Startup {
        version: VERSION_3 + 1,
        params: BTreeMap::from([("user".to_string(), "mz".to_string())]),
    }
    .encode(&mut frame)
    .unwrap();

    let mut stream = TcpStream::connect(pgwire_addr).await.unwrap();
    stream.write_all(&frame).await.unwrap();
    stream.flush().await.unwrap();

    let mut tag = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut tag))
        .await
        .expect("balancerd should answer rather than leave the client waiting")
        .unwrap();
    assert_eq!(
        (read, tag),
        (1, [b'E']),
        "expected an ErrorResponse, got {read} bytes; a rejection that is not flushed reaches \
         the client as a bare close",
    );
}

/// Returns the protocol the TLS server at `addr` selects for a client offering
/// `alpn`, in OpenSSL wire format (length-prefixed protocol names).
async fn alpn_selected(addr: SocketAddr, alpn: &'static [u8]) -> Option<Vec<u8>> {
    // The handshake is blocking, and the server shares this runtime.
    mz_ore::task::spawn_blocking(
        || "alpn_probe",
        move || {
            let mut connector = SslConnector::builder(SslMethod::tls()).unwrap();
            connector.set_verify(SslVerifyMode::NONE);
            connector.set_alpn_protos(alpn).unwrap();
            let stream = connector
                .build()
                .configure()
                .unwrap()
                .verify_hostname(false)
                .use_server_name_indication(false)
                .connect("", std::net::TcpStream::connect(addr).unwrap())
                .unwrap();
            stream.ssl().selected_alpn_protocol().map(<[u8]>::to_vec)
        },
    )
    .await
}
