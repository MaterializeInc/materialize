// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for balancerd.

use async_trait::async_trait;
use domain::resolv::StubResolver;
use mz_ore::tracing::TracingHandle;
use std::collections::{BTreeMap, btree_map};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::StreamExt;
use jsonwebtoken::{DecodingKey, EncodingKey};
use mz_balancerd::{
    BUILD_INFO, BackendResolverConfig, BalancerConfig, BalancerService, CancellationResolver,
    FronteggResolverConfig, ServiceResolver,
};
use mz_environmentd::test_util::{self, Ca, make_pg_tls};
use mz_frontegg_auth::{
    Authenticator as FronteggAuthentication, AuthenticatorConfig as FronteggConfig,
    DEFAULT_REFRESH_DROP_FACTOR, DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
};
use mz_frontegg_mock::{FronteggMockServer, models::ApiToken, models::UserConfig};
use mz_ore::cast::CastFrom;
use mz_ore::id_gen::{conn_id_org_uuid, org_id_conn_bits};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::retry::Retry;
use mz_ore::{assert_contains, assert_err, assert_ok, task};
use mz_server_core::TlsCertConfig;
use openssl::ssl::{SslConnectorBuilder, SslVerifyMode};
use openssl::x509::X509;
use tokio::sync::oneshot;
use tracing::error;
use uuid::Uuid;

#[derive(Clone)]
struct MockResolver {
    map: BTreeMap<String, Vec<String>>,
}

impl MockResolver {
    fn new(map: BTreeMap<String, Vec<String>>) -> MockResolver {
        Self { map }
    }
}

#[async_trait]
impl ServiceResolver for MockResolver {
    async fn resolve_arec(&self, name: &str) -> Result<Vec<String>, anyhow::Error> {
        Ok(self
            .map
            .get(name)
            .expect("Must be provided in testcase")
            .to_owned())
    }
    async fn resolve_cname(&self, name: &str) -> Result<Vec<String>, anyhow::Error> {
        Ok(self
            .map
            .get(name)
            .expect("Must be provided in testcase")
            .to_owned())
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
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
        .with_metrics_registry(metrics_registry.clone());
    let envid = config.environment_id.clone();
    let envd_server = config.clone().start().await;

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

    let test_resolvers = vec![
        // (
        //     BackendResolverConfig {
        //         https_static_resolver: Some(format!(
        //             "localhost:{}",
        //             envd_server.sql_local_addr().port()
        //         )),
        //         pgwire_static_resolver: Some(format!(
        //             "localhost:{}",
        //             envd_server.sql_local_addr().port()
        //         )),
        //         sni_resolver: None,
        //         frontegg_resolver: None,
        //         https_backend_port: envd_server.http_local_addr().port(),
        //         pgwire_backend_port: envd_server.sql_local_addr().port(),
        //     },
        //     CancellationResolver::Static(envd_server.sql_local_addr().to_string()),
        // ),
        // (
        //     BackendResolverConfig {
        //         https_static_resolver: Some(format!(
        //             "localhost:{}",
        //             envd_server.sql_local_addr().port()
        //         )),
        //         pgwire_static_resolver: None,
        //         sni_resolver: None,
        //         frontegg_resolver: Some(FronteggResolverConfig {
        //             auth: frontegg_auth,
        //             addr_template: envd_server.sql_local_addr().to_string(),
        //         }),
        //         https_backend_port: envd_server.http_local_addr().port(),
        //         pgwire_backend_port: envd_server.sql_local_addr().port(),
        //     },
        //     CancellationResolver::Directory(cancel_dir.path().to_owned()),
        // ),
        (
            BackendResolverConfig {
                https_static_resolver: None,
                pgwire_static_resolver: None,
                sni_resolver: Some("127.0.01".to_string()),
                frontegg_resolver: None,
                https_backend_port: envd_server.http_local_addr().port(),
                pgwire_backend_port: envd_server.sql_local_addr().port(),
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
        .danger_accept_invalid_certs(true)
        // No pool so that connections are never re-used which can use old ssl certs.
        .pool_max_idle_per_host(0)
        .tls_info(true)
        .build()
        .unwrap();

    for (resolver_config, cancellation_resolver) in test_resolvers {
        let (mut reload_tx, reload_rx) = futures::channel::mpsc::channel(1);
        let ticker = Box::pin(reload_rx);
        let is_frontegg_resolver = resolver_config.frontegg_resolver.is_some();

        // Create ResolverConfig from the old resolver
        let balancer_cfg = BalancerConfig::new(
            &BUILD_INFO,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            cancellation_resolver,
            resolver_config,
            MockResolver::new(BTreeMap::from([(
                "12345.localhost".to_string(),
                vec!["environmentd.environment-12345-0.svc.cluster.local".to_string()],
            )])),
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
            // tracing_handle,
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
            // balancer_pgwire_listen.ip(),
            "12345.localhost",
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

        // Assert cancellation is propagated.
        let cancel = pg_client.cancel_token();
        let copy = pg_client
            .copy_out("copy (subscribe (select * from mz_kafka_sinks)) to stdout")
            .await
            .unwrap();
        let _ = cancel.cancel_query(tls).await;
        let e = pin!(copy).next().await.unwrap().unwrap_err();
        assert_contains!(e.to_string(), "canceling statement due to user request");

        // Various tests about reloading of certs.

        // Assert the current certificate is as expected.
        let https_url = format!(
            "https://{host}:{port}/api/sql",
            host = "localhost",
            port = balancer_https_listen.port()
        );
        error!(
            "https://{host}:{port}/api/sql",
            host = balancer_https_listen.ip(),
            port = balancer_https_listen.port()
        ); // TODO remove
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

        if !is_frontegg_resolver {
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
                    handle.await.unwrap();
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
