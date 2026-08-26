// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for mutual TLS client authentication.
//!
//! These cover the path where the client connects to `environmentd` directly, so
//! the certificate `environmentd` evaluates is the one presented on its own
//! handshake. The path through `balancerd`, where the chain is forwarded, is
//! covered in `mz-balancerd`'s tests.

#![recursion_limit = "256"]

use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;

use mz_environmentd::test_util::{self, Ca, make_pg_tls};
use mz_ore::error::ErrorExt;
use mz_ore::{assert_contains, assert_err};
use postgres::config::SslMode;

/// Reads a PEM file into a string, for embedding in `ALTER SYSTEM SET`.
fn read_pem(path: impl AsRef<Path>) -> String {
    std::fs::read_to_string(path).expect("reading PEM")
}

/// A server that asks for client certificates, plus the CA that signs the
/// clients the test intends to admit.
struct Fixture {
    server: test_util::TestServer,
    client_ca: Ca,
    other_ca: Ca,
    server_ca: Ca,
}

impl Fixture {
    /// Starts a server with TLS and client certificate requests enabled, and
    /// `mtls_mode` / `mtls_client_ca` as given.
    ///
    /// The client authority is deliberately a *different* root from the one that
    /// issued the server's certificate, so a test cannot pass by accident
    /// through the server's own chain.
    async fn new(mode: &str, trust_client_ca: bool) -> Fixture {
        let server_ca = Ca::new_root("server ca").unwrap();
        let (server_cert, server_key) = server_ca
            .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .unwrap();
        let client_ca = Ca::new_root("client ca").unwrap();
        let other_ca = Ca::new_root("some other ca").unwrap();

        let mut harness = test_util::TestHarness::default()
            .with_tls(server_cert, server_key)
            .with_client_cert_requests()
            .with_system_parameter_default("mtls_mode".into(), mode.into());
        if trust_client_ca {
            harness = harness.with_system_parameter_default(
                "mtls_client_ca".into(),
                read_pem(client_ca.ca_cert_path()),
            );
        }
        Fixture {
            server: harness.start().await,
            client_ca,
            other_ca,
            server_ca,
        }
    }

    /// Connects as `materialize`, presenting `cert`/`key` if given.
    async fn connect(
        &self,
        identity: Option<(&Path, &Path)>,
    ) -> Result<tokio_postgres::Client, postgres::Error> {
        self.connect_inner(identity, false).await
    }

    /// Connects to the internal port as `mz_system`, without a client
    /// certificate.
    ///
    /// `with_tls` enables TLS on every listener, so this still negotiates TLS.
    /// What it does not do is present a client certificate, which is the point:
    /// internal users are exempt from the mutual TLS policy.
    async fn admin(&self) -> tokio_postgres::Client {
        self.connect_inner(None, true)
            .await
            .expect("internal connection")
    }

    async fn connect_inner(
        &self,
        identity: Option<(&Path, &Path)>,
        internal: bool,
    ) -> Result<tokio_postgres::Client, postgres::Error> {
        let ca_path = self.server_ca.ca_cert_path();
        let identity = identity.map(|(c, k)| (c.to_owned(), k.to_owned()));
        let tls = make_pg_tls(move |b| {
            b.set_ca_file(&ca_path)?;
            if let Some((cert, key)) = &identity {
                b.set_certificate_chain_file(cert)?;
                b.set_private_key_file(key, openssl::ssl::SslFiletype::PEM)?;
            }
            Ok(())
        });
        let builder = self.server.connect().ssl_mode(SslMode::Require);
        let builder = if internal {
            builder.internal()
        } else {
            builder
        };
        builder.with_tls(tls).await
    }
}

/// A trusted certificate admits the connection; one from another authority and
/// no certificate at all are both refused.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl`
async fn test_mtls_require_admits_only_trusted_certs() {
    let fixture = Fixture::new("require", true).await;

    let (cert, key) = fixture.client_ca.request_client_cert("client").unwrap();
    let client = fixture
        .connect(Some((&cert, &key)))
        .await
        .expect("trusted client certificate admitted");
    let row = client.query_one("SELECT current_user", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "materialize");

    let err = fixture
        .connect(None)
        .await
        .expect_err("connection without a certificate refused");
    assert_contains!(
        err.to_string_with_causes(),
        "a client certificate is required"
    );

    let (other_cert, other_key) = fixture.other_ca.request_client_cert("client").unwrap();
    let err = fixture
        .connect(Some((&other_cert, &other_key)))
        .await
        .expect_err("certificate from an untrusted authority refused");
    assert_contains!(
        err.to_string_with_causes(),
        "client certificate is not trusted"
    );
}

/// `require` with no configured anchors denies rather than admitting. A
/// misconfiguration must not open the door.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl`
async fn test_mtls_require_without_anchors_denies() {
    let fixture = Fixture::new("require", false).await;
    let (cert, key) = fixture.client_ca.request_client_cert("client").unwrap();

    assert_err!(fixture.connect(None).await);
    assert_err!(fixture.connect(Some((&cert, &key))).await);
}

/// `disable`, the default, ignores certificates entirely: a connection with no
/// certificate works, and so does one from an authority nobody trusts.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl`
async fn test_mtls_disabled_ignores_certificates() {
    let fixture = Fixture::new("disable", false).await;
    fixture
        .connect(None)
        .await
        .expect("no certificate is fine when disabled");

    let (cert, key) = fixture.other_ca.request_client_cert("client").unwrap();
    fixture
        .connect(Some((&cert, &key)))
        .await
        .expect("an untrusted certificate is ignored when disabled");
}

/// `allow` is the rollout mode: it admits clients that have not yet been issued
/// certificates, while still refusing one from the wrong authority. Refusing the
/// latter is what makes the mode usable for validating a rollout.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl`
async fn test_mtls_allow_tolerates_absent_certificates() {
    let fixture = Fixture::new("allow", true).await;

    fixture
        .connect(None)
        .await
        .expect("no certificate admitted under allow");

    let (cert, key) = fixture.client_ca.request_client_cert("client").unwrap();
    fixture
        .connect(Some((&cert, &key)))
        .await
        .expect("trusted certificate admitted under allow");

    let (other_cert, other_key) = fixture.other_ca.request_client_cert("client").unwrap();
    assert_err!(
        fixture.connect(Some((&other_cert, &other_key))).await,
        "an untrusted certificate is still refused under allow"
    );
}

/// The mode and the anchors are runtime configuration: flipping `disable` to
/// `require` closes the door on the next connection, without a restart. This is
/// the property that makes the feature operable, so it is tested end to end
/// rather than only at the policy layer.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl`
async fn test_mtls_mode_changes_take_effect_without_restart() {
    let fixture = Fixture::new("disable", true).await;

    fixture
        .connect(None)
        .await
        .expect("admitted while mtls is disabled");

    let admin = fixture.admin().await;
    admin
        .batch_execute("ALTER SYSTEM SET mtls_mode = 'require'")
        .await
        .expect("setting mtls_mode");

    let err = fixture
        .connect(None)
        .await
        .expect_err("refused once mtls is required");
    assert_contains!(
        err.to_string_with_causes(),
        "a client certificate is required"
    );

    let (cert, key) = fixture.client_ca.request_client_cert("client").unwrap();
    fixture
        .connect(Some((&cert, &key)))
        .await
        .expect("trusted certificate admitted after the switch");

    // Rotating the anchors to a different authority revokes the certificate that
    // just worked, again without a restart.
    admin
        .batch_execute(&format!(
            "ALTER SYSTEM SET mtls_client_ca = '{}'",
            read_pem(fixture.other_ca.ca_cert_path())
        ))
        .await
        .expect("rotating mtls_client_ca");
    assert_err!(
        fixture.connect(Some((&cert, &key))).await,
        "the previous authority is no longer trusted"
    );
    let (other_cert, other_key) = fixture.other_ca.request_client_cert("client").unwrap();
    fixture
        .connect(Some((&other_cert, &other_key)))
        .await
        .expect("the new authority is trusted");
}

/// With `common-name` binding, the leaf's CN must equal the connecting username.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl`
async fn test_mtls_common_name_binding() {
    let fixture = Fixture::new("require", true).await;
    let admin = fixture.admin().await;
    admin
        .batch_execute("ALTER SYSTEM SET mtls_identity_binding = 'common-name'")
        .await
        .expect("setting mtls_identity_binding");

    // A certificate issued to `materialize` may connect as `materialize`.
    let (cert, key) = fixture
        .client_ca
        .request_client_cert("materialize")
        .unwrap();
    fixture
        .connect(Some((&cert, &key)))
        .await
        .expect("matching common name admitted");

    // A certificate from the same trusted authority, issued to somebody else,
    // may not. Without the binding this certificate would be admitted, so this
    // is what distinguishes a second factor from a shared door key.
    let (other_cert, other_key) = fixture
        .client_ca
        .request_client_cert("someone-else")
        .unwrap();
    let err = fixture
        .connect(Some((&other_cert, &other_key)))
        .await
        .expect_err("mismatched common name refused");
    assert_contains!(
        err.to_string_with_causes(),
        "client certificate does not match the requested user"
    );
}

/// A chain through an intermediate authority validates when the client sends the
/// intermediate, and fails when it sends only its leaf.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl`
async fn test_mtls_intermediate_chain() {
    let fixture = Fixture::new("require", true).await;
    let intermediate = fixture.client_ca.request_ca("intermediary").unwrap();
    let (leaf_cert, leaf_key) = intermediate.request_client_cert("client").unwrap();

    // Leaf alone: the server holds only the root, so the chain is incomplete.
    let err = fixture
        .connect(Some((&leaf_cert, &leaf_key)))
        .await
        .expect_err("incomplete chain refused");
    assert_contains!(
        err.to_string_with_causes(),
        "client certificate is not trusted"
    );

    // Leaf plus intermediate, concatenated as a chain file, validates.
    let chain_path = intermediate.dir.path().join("client-chain.crt");
    let chain = format!(
        "{}{}",
        read_pem(&leaf_cert),
        read_pem(intermediate.ca_cert_path())
    );
    std::fs::write(&chain_path, chain).unwrap();
    fixture
        .connect(Some((&chain_path, &leaf_key)))
        .await
        .expect("complete chain admitted");
}

/// Internal users are exempt, matching the network policy carve-out: internal
/// listeners are secured by the deployment rather than by tenant SQL, and
/// locking `mz_system` out of a misconfigured environment would leave no way to
/// fix it.
#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `OPENSSL_init_ssl`
async fn test_mtls_does_not_apply_to_internal_users() {
    let fixture = Fixture::new("require", true).await;
    // Admitted despite presenting no client certificate under `require`.
    let admin = fixture.admin().await;
    admin
        .batch_execute("SELECT 1")
        .await
        .expect("internal session works");
}
