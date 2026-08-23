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
use mz_ore::now::SYSTEM_TIME;
use mz_ore::retry::Retry;
use mz_ore::tracing::TracingHandle;
use mz_ore::{assert_contains, assert_err, assert_ok, task};
use mz_pgwire_common::MZ_CLIENT_CERT_KEY;
use mz_server_core::{ClientCertMode, TlsCertConfig};
use openssl::ssl::{SslConnectorBuilder, SslVerifyMode};
use openssl::x509::X509;
use postgres::config::SslMode;
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
        client_certs: ClientCertMode::Disable,
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
            None,
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

/// Mutual TLS end to end through the balancer.
///
/// This is the case the whole design exists for: the balancer terminates the
/// client's TLS, so it is the only party that can prove the client holds its
/// certificate's private key, while `environmentd` is the only party that knows
/// which issuers the tenant trusts. The balancer forwards the chain and
/// `environmentd` judges it.
///
/// Four certificate authorities are in play, and keeping them distinct is the
/// point of the test:
///
/// * `server_ca` issues the balancer's and environmentd's serving certificates.
/// * `proxy_ca` issues the balancer's *client* identity, which is what lets
///   environmentd believe a forwarded certificate.
/// * `client_ca` issues end-client certificates and is what the tenant
///   configures as `mtls_client_ca`.
/// * `rogue_ca` issues certificates nobody should accept.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
#[allow(clippy::disallowed_methods)]
async fn test_balancer_mtls_forwarding() {
    let server_ca = Ca::new_root("server ca").unwrap();
    let (server_cert, server_key) = server_ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let proxy_ca = Ca::new_root("proxy ca").unwrap();
    let (proxy_cert, proxy_key) = proxy_ca.request_client_cert("balancerd").unwrap();
    let client_ca = Ca::new_root("client ca").unwrap();
    let rogue_ca = Ca::new_root("rogue ca").unwrap();

    // environmentd: requests client certificates, trusts `client_ca` for end
    // clients and `proxy_ca` for proxies, and requires a certificate.
    let envd_server = test_util::TestHarness::default()
        .with_tls(server_cert.clone(), server_key.clone())
        .with_client_cert_requests()
        .with_tls_proxy_ca(proxy_ca.ca_cert_path())
        .with_system_parameter_default("mtls_mode".into(), "require".into())
        .with_system_parameter_default(
            "mtls_client_ca".into(),
            std::fs::read_to_string(client_ca.ca_cert_path()).unwrap(),
        )
        .start()
        .await;

    let cancel_dir = tempfile::tempdir().unwrap();
    let balancer_cfg = BalancerConfig::new(
        &BUILD_INFO,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        CancellationResolver::Directory(cancel_dir.path().to_owned()),
        BalancerResolver::Static(envd_server.sql_local_addr().to_string()),
        envd_server.http_local_addr().to_string(),
        // The balancer asks clients for certificates so it has something to
        // forward.
        Some(TlsCertConfig {
            cert: server_cert.clone(),
            key: server_key.clone(),
            client_certs: ClientCertMode::Request,
        }),
        // Internal TLS, presenting the proxy identity: without this environmentd
        // has no reason to believe anything the balancer forwards.
        true,
        Some(TlsCertConfig {
            cert: proxy_cert.clone(),
            key: proxy_key.clone(),
            client_certs: ClientCertMode::Disable,
        }),
        MetricsRegistry::new(),
        Box::pin(futures::stream::empty()),
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
    let balancer_addr = balancer_server.pgwire.0.local_addr();
    task::spawn(|| "balancer", async {
        balancer_server.serve().await.unwrap();
    });

    // Connects through the balancer, optionally presenting a client identity.
    let connect = |identity: Option<(std::path::PathBuf, std::path::PathBuf)>| {
        let conn_str = format!(
            "user=materialize host={} port={} sslmode=require",
            balancer_addr.ip(),
            balancer_addr.port()
        );
        async move {
            let tls = make_pg_tls(move |b: &mut SslConnectorBuilder| {
                // The balancer's serving certificate is not the subject of this
                // test, so skip verifying it.
                b.set_verify(SslVerifyMode::NONE);
                if let Some((cert, key)) = &identity {
                    b.set_certificate_chain_file(cert)?;
                    b.set_private_key_file(key, openssl::ssl::SslFiletype::PEM)?;
                }
                Ok(())
            });
            let (client, conn) = tokio_postgres::connect(&conn_str, tls).await?;
            task::spawn(|| "mtls-pg_client", async move {
                let _ = conn.await;
            });
            Ok::<_, tokio_postgres::Error>(client)
        }
    };

    // A client certificate from the trusted authority is admitted, having been
    // captured by the balancer and validated by environmentd.
    let (client_cert, client_key) = client_ca.request_client_cert("client").unwrap();
    let client = connect(Some((client_cert.clone(), client_key.clone())))
        .await
        .expect("trusted client certificate admitted through the balancer");
    let res: i32 = client.query_one("SELECT 3", &[]).await.unwrap().get(0);
    assert_eq!(res, 3);

    // No client certificate: the balancer forwards nothing, so environmentd
    // refuses under `require`.
    let err = connect(None)
        .await
        .expect_err("connection without a client certificate refused");
    assert_contains!(
        err.to_string_with_causes(),
        "a client certificate is required"
    );

    // A certificate from an authority the tenant does not trust is refused, even
    // though the balancer forwarded it perfectly well. The balancer deliberately
    // does not judge chains, so this rejection can only be environmentd's.
    let (rogue_cert, rogue_key) = rogue_ca.request_client_cert("client").unwrap();
    let err = connect(Some((rogue_cert, rogue_key)))
        .await
        .expect_err("certificate from an untrusted authority refused");
    assert_contains!(
        err.to_string_with_causes(),
        "client certificate is not trusted"
    );

    // The trust anchors are live configuration even with the balancer in the
    // path: rotating them revokes the certificate that just worked.
    //
    // The internal port negotiates TLS like every other listener here, since
    // `with_tls` enables it everywhere. It presents no client certificate,
    // which is fine: internal users are exempt from the policy.
    let admin = envd_server
        .connect()
        .internal()
        .ssl_mode(SslMode::Require)
        .with_tls(make_pg_tls(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        }))
        .await
        .unwrap();
    admin
        .batch_execute(&format!(
            "ALTER SYSTEM SET mtls_client_ca = '{}'",
            std::fs::read_to_string(rogue_ca.ca_cert_path()).unwrap()
        ))
        .await
        .unwrap();
    assert_err!(
        connect(Some((client_cert, client_key))).await,
        "the rotated-out authority is no longer trusted"
    );
}

/// A balancer that forwards a chain but cannot prove it is a proxy gets its
/// assertion ignored.
///
/// This is the fail-closed direction of the design. The client's certificate is
/// genuine and the balancer forwards it faithfully, but the balancer presents no
/// identity of its own, so `environmentd` has no way to distinguish it from any
/// other peer that can reach the port. It must therefore ignore the forwarded
/// chain and refuse the connection, rather than trusting an unauthenticated
/// assertion.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
#[allow(clippy::disallowed_methods)]
async fn test_forwarded_cert_ignored_from_unauthenticated_proxy() {
    let server_ca = Ca::new_root("server ca").unwrap();
    let (server_cert, server_key) = server_ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let proxy_ca = Ca::new_root("proxy ca").unwrap();
    let client_ca = Ca::new_root("client ca").unwrap();

    let envd_server = test_util::TestHarness::default()
        .with_tls(server_cert.clone(), server_key.clone())
        .with_client_cert_requests()
        .with_tls_proxy_ca(proxy_ca.ca_cert_path())
        .with_system_parameter_default("mtls_mode".into(), "require".into())
        .with_system_parameter_default(
            "mtls_client_ca".into(),
            std::fs::read_to_string(client_ca.ca_cert_path()).unwrap(),
        )
        .start()
        .await;

    let cancel_dir = tempfile::tempdir().unwrap();
    let balancer_cfg = BalancerConfig::new(
        &BUILD_INFO,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        CancellationResolver::Directory(cancel_dir.path().to_owned()),
        BalancerResolver::Static(envd_server.sql_local_addr().to_string()),
        envd_server.http_local_addr().to_string(),
        Some(TlsCertConfig {
            cert: server_cert.clone(),
            key: server_key.clone(),
            client_certs: ClientCertMode::Request,
        }),
        // Internal TLS on, but with no client identity: the balancer is
        // anonymous to environmentd.
        true,
        None,
        MetricsRegistry::new(),
        Box::pin(futures::stream::empty()),
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
    let balancer_addr = balancer_server.pgwire.0.local_addr();
    task::spawn(|| "balancer", async {
        balancer_server.serve().await.unwrap();
    });

    let (client_cert, client_key) = client_ca.request_client_cert("client").unwrap();
    let conn_str = format!(
        "user=materialize host={} port={} sslmode=require",
        balancer_addr.ip(),
        balancer_addr.port()
    );
    let tls = make_pg_tls(move |b: &mut SslConnectorBuilder| {
        b.set_verify(SslVerifyMode::NONE);
        b.set_certificate_chain_file(&client_cert)?;
        b.set_private_key_file(&client_key, openssl::ssl::SslFiletype::PEM)?;
        Ok(())
    });
    let err = match tokio_postgres::connect(&conn_str, tls).await {
        Ok(_) => panic!("a chain forwarded by an unauthenticated peer must be ignored"),
        Err(e) => e,
    };
    assert_contains!(
        err.to_string_with_causes(),
        "a client certificate is required"
    );
}

/// The balancer rejects a client that supplies `mz_client_cert` itself.
///
/// The parameter is the balancer's to set. A client that sets it is trying to
/// hand `environmentd` an identity it never proved, so the connection is refused
/// at the balancer rather than forwarded with the client's value overwritten.
/// This is the same treatment `mz_forwarded_for` and `mz_connection_uuid` get.
///
/// Driven with a hand-built startup packet because no Postgres client library
/// will send an arbitrary startup parameter.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
#[allow(clippy::disallowed_methods)]
async fn test_balancer_rejects_client_supplied_cert_param() {
    let server_ca = Ca::new_root("server ca").unwrap();
    let (server_cert, server_key) = server_ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();

    let envd_server = test_util::TestHarness::default()
        .with_tls(server_cert.clone(), server_key.clone())
        .start()
        .await;

    let cancel_dir = tempfile::tempdir().unwrap();
    let balancer_cfg = BalancerConfig::new(
        &BUILD_INFO,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        CancellationResolver::Directory(cancel_dir.path().to_owned()),
        BalancerResolver::Static(envd_server.sql_local_addr().to_string()),
        envd_server.http_local_addr().to_string(),
        Some(TlsCertConfig {
            cert: server_cert.clone(),
            key: server_key.clone(),
            client_certs: ClientCertMode::Request,
        }),
        false,
        None,
        MetricsRegistry::new(),
        Box::pin(futures::stream::empty()),
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
    let balancer_addr = balancer_server.pgwire.0.local_addr();
    task::spawn(|| "balancer", async {
        balancer_server.serve().await.unwrap();
    });

    let error = send_startup_over_tls(
        balancer_addr,
        BTreeMap::from([
            ("user".to_string(), "materialize".to_string()),
            (
                MZ_CLIENT_CERT_KEY.to_string(),
                "anything at all".to_string(),
            ),
        ]),
    )
    .await
    .expect("the server should answer with an error");
    assert_contains!(&error, MZ_CLIENT_CERT_KEY);
    assert_contains!(&error, "invalid parameter");
}

/// Opens a TLS pgwire connection to `addr`, sends a startup message with
/// `params`, and returns the text of the `ErrorResponse` the server replies
/// with, or `None` if the reply is not an error.
///
/// Speaks just enough of the protocol for the negative test above: the reply is
/// expected to be a single `ErrorResponse`, whose fields are NUL-terminated
/// key/value pairs.
async fn send_startup_over_tls(
    addr: SocketAddr,
    params: BTreeMap<String, String>,
) -> Option<String> {
    use bytes::BytesMut;
    use mz_pgwire_common::{ACCEPT_SSL_ENCRYPTION, FrontendStartupMessage, VERSION_3};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut buf = BytesMut::new();
    FrontendStartupMessage::SslRequest.encode(&mut buf).unwrap();
    stream.write_all(&buf).await.unwrap();
    let mut response = [0u8; 1];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(
        response,
        [ACCEPT_SSL_ENCRYPTION],
        "balancer should accept TLS"
    );

    let mut connector =
        openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
    connector.set_verify(SslVerifyMode::NONE);
    let mut ssl = connector
        .build()
        .configure()
        .unwrap()
        .into_ssl("balancer")
        .unwrap();
    ssl.set_connect_state();
    let mut stream = tokio_openssl::SslStream::new(ssl, stream).unwrap();
    std::pin::Pin::new(&mut stream).connect().await.unwrap();

    buf.clear();
    FrontendStartupMessage::Startup {
        version: VERSION_3,
        params,
    }
    .encode(&mut buf)
    .unwrap();
    stream.write_all(&buf).await.unwrap();

    // Read the tag and length, then the body.
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.ok()?;
    if header[0] != b'E' {
        return None;
    }
    let len = usize::cast_from(u32::from_be_bytes([
        header[1], header[2], header[3], header[4],
    ]));
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body).await.ok()?;
    Some(String::from_utf8_lossy(&body).replace('\0', " "))
}
