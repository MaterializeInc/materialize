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
use mz_pgwire_common::{FrontendStartupMessage, MAX_STARTUP_FRAME_SIZE, REJECT_ENCRYPTION};
use mz_server_core::TlsCertConfig;
use openssl::ssl::{SslConnectorBuilder, SslVerifyMode};
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
            vec![],
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
    }
}

/// Starts a balancerd whose pgwire listener is reachable but whose upstream is
/// not. These tests never get far enough to be forwarded anywhere.
async fn start_balancer() -> SocketAddr {
    let unreachable = "127.0.0.1:1".to_string();
    let (_reload_tx, reload_rx) = futures::channel::mpsc::channel(1);
    let balancer_cfg = BalancerConfig::new(
        &BUILD_INFO,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        CancellationResolver::Static(unreachable.clone()),
        BalancerResolver::Static(unreachable.clone()),
        unreachable.clone(),
        // No certificate. These connections are rejected or parked before TLS
        // would have come into it.
        None,
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
        vec![],
    );
    let balancer_server = BalancerService::new(balancer_cfg).await.unwrap();
    let pgwire_addr = balancer_server.pgwire.0.local_addr();
    task::spawn(|| "balancer", async {
        balancer_server.serve().await.unwrap();
    });
    pgwire_addr
}

/// Opens a pgwire connection, writes a startup frame-length header declaring
/// `frame_len` bytes, and sends nothing further.
async fn startup_header_only(addr: SocketAddr, frame_len: u32) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(&frame_len.to_be_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    stream
}

/// Whether balancerd closed the connection rather than waiting for a body.
async fn was_closed(stream: &mut TcpStream) -> bool {
    let mut byte = [0u8; 1];
    match tokio::time::timeout(Duration::from_secs(10), stream.read(&mut byte)).await {
        // Still waiting on us, so the frame was accepted.
        Err(_elapsed) => false,
        Ok(Ok(0)) => true,
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => true,
        Ok(Ok(n)) => panic!("balancerd sent {n} bytes instead of closing or waiting: {byte:?}"),
        Ok(Err(e)) => panic!("unexpected error reading from balancerd: {e}"),
    }
}

/// A startup frame larger than the startup budget is refused on the raw socket,
/// before TLS and before any login, so a client cannot make balancerd size a
/// buffer by declaring a length it never sends.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_pgwire_oversized_startup_frame_is_rejected() {
    let pgwire_addr = start_balancer().await;
    let budget = u32::try_from(MAX_STARTUP_FRAME_SIZE).expect("fits in a frame-length field");
    let protocol_max = u32::try_from(MAX_FRAME_SIZE).expect("fits in a frame-length field");

    for declared in [budget + 1, protocol_max] {
        let mut stream = startup_header_only(pgwire_addr, declared).await;
        assert!(
            was_closed(&mut stream).await,
            "balancerd accepted a {declared} byte startup frame and waited for the body",
        );
    }

    // The boundary itself is still served, so the rejection is the budget doing
    // its job rather than balancerd refusing everything.
    let mut stream = startup_header_only(pgwire_addr, budget).await;
    assert!(
        !was_closed(&mut stream).await,
        "balancerd rejected a startup frame at the budget",
    );
}

/// Connections that declare a frame within budget and then go silent are still
/// held open indefinitely, with no handshake deadline and no ceiling on how many
/// one peer may hold. Bounding the frame caps what one such connection costs,
/// not how many of them exist.
///
/// TODO(CLO-272): assert a bound here once the pre-startup limit lands.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_pgwire_startup_preauth_connections_are_held() {
    let pgwire_addr = start_balancer().await;

    const HELD_CONNS: usize = 16;
    let mut held = Vec::new();
    for _ in 0..HELD_CONNS {
        held.push(startup_header_only(pgwire_addr, 1 << 10).await);
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    let still_held = futures::future::join_all(held.iter_mut().map(|stream| async move {
        let mut byte = [0u8; 1];
        tokio::time::timeout(Duration::from_millis(100), stream.read(&mut byte)).await
    }))
    .await;
    for (i, outcome) in still_held.into_iter().enumerate() {
        assert!(
            outcome.is_err(),
            "balancerd released unauthenticated connection {i} instead of holding it: {outcome:?}",
        );
    }

    // The listener is still accepting and still answering past the flood, so no
    // accept-time limit applies and this is a memory concern rather than a
    // wedged listener.
    //
    // This also rules out the only way the assertions above could pass without
    // reproducing anything: the listening socket is bound in
    // `BalancerService::new`, before `serve` runs its accept loop, so connections
    // that were merely sitting in the accept backlog would also read as "held".
    // A startup message answered here proves the accept loop is draining.
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
