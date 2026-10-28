// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TLS, SASL, re-authentication, address rewriting and reconnect tests.
//!
//! The broker tests run when `MZ_KAFKA_CLIENT_TEST_AUTH_SECRETS` names a
//! directory with the certificates of `test/test-certs` (`ca.crt`,
//! `materialized.{crt,key,p12}`, ...) and a broker configured like
//! `test/kafka-auth` listens on 127.0.0.1 with listeners advertised as
//! `kafka:39092` (plaintext), `39093` (SSL), `39094` (mTLS), `39095`
//! (SASL_PLAINTEXT, with `connections.max.reauth.ms=10000` for PLAIN),
//! `39096` (SASL_SSL) and `39097` (SASL mTLS). The resolve hook maps `kafka`
//! to 127.0.0.1, while TLS verifies the name `kafka`.

use std::io;
use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::client::{Client, ClientContext, OAuthToken};
use crate::config::settings::{ClientKind, ClientSettings};
use crate::config::{ClientConfig, RDKafkaLogLevel};
use crate::error::KafkaError;
use crate::internal::runtime;
use crate::types::RDKafkaErrorCode;

/// A context that records logs and errors and rewrites broker addresses.
#[derive(Default)]
struct Ctx {
    logs: Mutex<Vec<(RDKafkaLogLevel, String, String)>>,
    errors: Mutex<Vec<String>>,
    /// Resolve every broker to 127.0.0.1 and this port, if set.
    redirect: AtomicU16,
    resolves: Mutex<Vec<String>>,
}

impl ClientContext for Arc<Ctx> {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.logs
            .lock()
            .unwrap()
            .push((level, fac.to_string(), log_message.to_string()));
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.errors
            .lock()
            .unwrap()
            .push(format!("{error}: {reason}"));
    }

    fn resolve_broker_addr(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        assert!(!runtime::is_io_thread(), "resolve hook on an I/O thread");
        self.resolves.lock().unwrap().push(format!("{host}:{port}"));
        let redirect = self.redirect.load(Ordering::SeqCst);
        if redirect != 0 {
            return Ok(vec![SocketAddr::from(([127, 0, 0, 1], redirect))]);
        }
        let host = if host == "kafka" { "127.0.0.1" } else { host };
        (host, port).to_socket_addrs().map(|a| a.collect())
    }
}

impl Ctx {
    fn fail_lines(&self) -> Vec<String> {
        self.logs
            .lock()
            .unwrap()
            .iter()
            .filter(|(level, fac, _)| fac == "FAIL" && *level != RDKafkaLogLevel::Debug)
            .map(|(_, _, m)| m.clone())
            .collect()
    }
}

fn secrets() -> Option<String> {
    std::env::var("MZ_KAFKA_CLIENT_TEST_AUTH_SECRETS").ok()
}

fn read(secrets: &str, file: &str) -> String {
    std::fs::read_to_string(format!("{secrets}/{file}")).unwrap()
}

fn client(pairs: &[(&str, &str)]) -> Result<(Client<Arc<Ctx>>, Arc<Ctx>), KafkaError> {
    let mut config = ClientConfig::new();
    config.set_log_level(RDKafkaLogLevel::Info);
    config.set("reconnect.backoff.ms", "10");
    config.set("reconnect.backoff.max.ms", "100");
    for (k, v) in pairs {
        config.set(*k, *v);
    }
    let settings = ClientSettings::from_config(&config, ClientKind::Consumer)?;
    let ctx = Arc::new(Ctx::default());
    let client = Client::new(settings, Arc::clone(&ctx))?;
    Ok((client, ctx))
}

/// Fetches metadata and returns the result, the FAIL lines and the errors.
fn fetch(
    pairs: &[(&str, &str)],
    timeout: Duration,
) -> (Result<usize, KafkaError>, Vec<String>, Vec<String>) {
    let (client, ctx) = client(pairs).unwrap();
    let result = client
        .fetch_metadata(None, timeout)
        .map(|md| md.brokers().len());
    let fails = ctx.fail_lines();
    // Deliver the errors, as a consumer poll would.
    client.cluster().events().serve();
    let errors = ctx.errors.lock().unwrap().clone();
    (result, fails, errors)
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn tls_listeners() {
    let Some(s) = secrets() else { return };
    let ca = read(&s, "ca.crt");
    let cert = read(&s, "materialized.crt");
    let key = read(&s, "materialized.key");
    let ok = Duration::from_secs(10);
    let fail = Duration::from_millis(500);

    // SNI and verification use "kafka" although the hook resolves it to
    // 127.0.0.1.
    let (r, fails, _) = fetch(
        &[
            ("bootstrap.servers", "kafka:39093"),
            ("security.protocol", "ssl"),
            ("ssl.ca.pem", &ca),
        ],
        ok,
    );
    assert_eq!(r, Ok(1), "{fails:#?}");

    // The certificate is not valid for "localhost".
    let (r, fails, _) = fetch(
        &[
            ("bootstrap.servers", "localhost:39093"),
            ("security.protocol", "ssl"),
            ("ssl.ca.pem", &ca),
        ],
        fail,
    );
    assert!(r.is_err());
    assert!(
        fails[0].contains("SSL handshake failed: error:0A000086:SSL routines::certificate verify failed: broker certificate could not be verified"),
        "{fails:#?}"
    );
    // ... which does not matter without endpoint identification.
    let (r, _, _) = fetch(
        &[
            ("bootstrap.servers", "localhost:39093"),
            ("security.protocol", "ssl"),
            ("ssl.ca.pem", &ca),
            ("ssl.endpoint.identification.algorithm", "none"),
        ],
        ok,
    );
    assert_eq!(r, Ok(1));

    // Untrusted CA.
    let (_, fails, errors) = fetch(
        &[
            ("bootstrap.servers", "kafka:39093"),
            ("security.protocol", "ssl"),
        ],
        fail,
    );
    assert!(
        fails[0].contains("broker certificate could not be verified"),
        "{fails:#?}"
    );
    assert!(errors[0].starts_with("Global error: SSL (Local: SSL error): ssl://kafka:39093/bootstrap: SSL handshake failed: "), "{errors:#?}");

    // TLS to a plaintext listener.
    let (_, fails, _) = fetch(
        &[
            ("bootstrap.servers", "kafka:39092"),
            ("security.protocol", "ssl"),
            ("ssl.ca.pem", &ca),
        ],
        fail,
    );
    assert!(
        fails[0].starts_with(
            "[thrd:ssl://kafka:39092/bootstrap]: ssl://kafka:39092/bootstrap: SSL handshake \
             failed: Disconnected: SSL connection closed by peer: connecting to a PLAINTEXT \
             broker listener? (after "
        ),
        "{fails:#?}"
    );

    // Plaintext to a TLS listener.
    let (_, fails, _) = fetch(&[("bootstrap.servers", "kafka:39093")], fail);
    assert!(
        fails[0].contains(
            "kafka:39093/bootstrap: Disconnected: connection closed by peer: receive 0 after \
             POLLIN (after "
        ),
        "{fails:#?}"
    );

    // mTLS with and without a client certificate, PEM and PKCS#12.
    let (r, fails, _) = fetch(
        &[
            ("bootstrap.servers", "kafka:39094"),
            ("security.protocol", "ssl"),
            ("ssl.ca.pem", &ca),
            ("ssl.certificate.pem", &cert),
            ("ssl.key.pem", &key),
        ],
        ok,
    );
    assert_eq!(r, Ok(1), "{fails:#?}");
    let keystore = format!("{s}/materialized.p12");
    let ca_location = format!("{s}/ca.crt");
    let (r, fails, _) = fetch(
        &[
            ("bootstrap.servers", "kafka:39094"),
            ("security.protocol", "ssl"),
            ("ssl.ca.location", &ca_location),
            ("ssl.keystore.location", &keystore),
            ("ssl.keystore.password", "mzmzmz"),
        ],
        ok,
    );
    assert_eq!(r, Ok(1), "{fails:#?}");
    let (_, fails, _) = fetch(
        &[
            ("bootstrap.servers", "kafka:39094"),
            ("security.protocol", "ssl"),
            ("ssl.ca.pem", &ca),
        ],
        fail,
    );
    assert!(
        fails[0].contains("Receive failed: error:0A00045C:SSL routines::tlsv13 alert certificate required: SSL alert number 116"),
        "{fails:#?}"
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn sasl_listeners() {
    let Some(s) = secrets() else { return };
    let ca = read(&s, "ca.crt");
    let cert = read(&s, "materialized.crt");
    let key = read(&s, "materialized.key");
    let ok = Duration::from_secs(10);
    for (port, protocol) in [
        ("39095", "sasl_plaintext"),
        ("39096", "sasl_ssl"),
        ("39097", "sasl_ssl"),
    ] {
        let bootstrap = format!("kafka:{port}");
        for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] {
            let mut pairs = vec![
                ("bootstrap.servers", bootstrap.as_str()),
                ("security.protocol", protocol),
                ("sasl.mechanisms", mechanism),
                ("sasl.username", "materialize"),
                ("sasl.password", "sekurity"),
                ("ssl.ca.pem", ca.as_str()),
            ];
            if port == "39097" {
                pairs.push(("ssl.certificate.pem", &cert));
                pairs.push(("ssl.key.pem", &key));
            }
            let (r, fails, _) = fetch(&pairs, ok);
            assert_eq!(r, Ok(1), "{port} {mechanism}: {fails:#?}");

            let mut pairs = pairs.clone();
            pairs[4] = ("sasl.password", "wrong");
            let (r, fails, errors) = fetch(&pairs, Duration::from_millis(1500));
            assert!(r.is_err());
            let expected = if mechanism == "PLAIN" {
                "SASL authentication error: Authentication failed: Invalid username or password (after ".to_string()
            } else {
                format!(
                    "SASL authentication error: Authentication failed during authentication due to \
                     invalid credentials with SASL mechanism {mechanism} (after "
                )
            };
            assert!(
                fails[0].contains(&expected),
                "{port} {mechanism}: {fails:#?}"
            );
            assert!(
                errors[0]
                    .starts_with("Global error: Authentication (Local: Authentication failure): "),
                "{errors:#?}"
            );
        }
    }

    // SASL against a plaintext listener.
    let (_, fails, _) = fetch(
        &[
            ("bootstrap.servers", "kafka:39092"),
            ("security.protocol", "sasl_plaintext"),
            ("sasl.mechanisms", "PLAIN"),
            ("sasl.username", "materialize"),
            ("sasl.password", "sekurity"),
        ],
        Duration::from_millis(500),
    );
    assert!(
        fails[0].contains(
            "SASL PLAIN mechanism handshake failed: Broker: Request not valid in current SASL \
             state: broker's supported mechanisms:  (after "
        ),
        "{fails:#?}"
    );
}

/// A context with an OAUTHBEARER token source.
#[derive(Default)]
struct OAuthCtx {
    calls: Mutex<Vec<Option<String>>>,
    errors: Mutex<Vec<String>>,
    fail: AtomicBool,
}

impl ClientContext for Arc<OAuthCtx> {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn error(&self, error: KafkaError, reason: &str) {
        self.errors
            .lock()
            .unwrap()
            .push(format!("{error}: {reason}"));
    }

    fn resolve_broker_addr(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        let host = if host == "kafka" { "127.0.0.1" } else { host };
        (host, port).to_socket_addrs().map(|a| a.collect())
    }

    fn generate_oauth_token(
        &self,
        config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        assert!(!runtime::is_io_thread());
        self.calls.lock().unwrap().push(config.map(str::to_string));
        if self.fail.load(Ordering::SeqCst) {
            return Err("no credentials".into());
        }
        let now = crate::util::current_time_millis();
        Ok(OAuthToken {
            token: "token".into(),
            principal_name: String::new(),
            // Refresh after 80% of 1 s.
            lifetime_ms: now + 1000,
        })
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn oauthbearer_tokens() {
    let ctx = Arc::new(OAuthCtx::default());
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", "kafka:39095")
        .set("security.protocol", "sasl_plaintext")
        .set("sasl.mechanisms", "OAUTHBEARER")
        .set("sasl.oauthbearer.config", "cfg");
    let settings = ClientSettings::from_config(&config, ClientKind::Consumer).unwrap();
    let client = Client::new(settings, Arc::clone(&ctx)).unwrap();
    // The first token is requested at creation, the next at 80% of its
    // lifetime.
    let start = Instant::now();
    while ctx.calls.lock().unwrap().len() < 3 {
        assert!(start.elapsed() < Duration::from_secs(10));
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(start.elapsed() >= Duration::from_millis(1500));
    assert_eq!(ctx.calls.lock().unwrap()[0].as_deref(), Some("cfg"));
    if secrets().is_some() {
        // The broker does not enable OAUTHBEARER.
        let err = client
            .fetch_metadata(None, Duration::from_millis(500))
            .unwrap_err();
        assert_eq!(
            err,
            KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerTransportFailure)
        );
        client.cluster().events().serve();
        let errors = ctx.errors.lock().unwrap().clone();
        assert!(
            errors.iter().any(|e| e.contains(
                "SASL OAUTHBEARER mechanism handshake failed: Broker: Unsupported SASL \
                 mechanism: broker's supported mechanisms: "
            )),
            "{errors:#?}"
        );
    }
    drop(client);

    // Failures are reported once per distinct error.
    let ctx = Arc::new(OAuthCtx::default());
    ctx.fail.store(true, Ordering::SeqCst);
    let settings = ClientSettings::from_config(&config, ClientKind::Consumer).unwrap();
    let client = Client::new(settings, Arc::clone(&ctx)).unwrap();
    let start = Instant::now();
    while ctx.errors.lock().unwrap().is_empty() {
        assert!(start.elapsed() < Duration::from_secs(10));
        std::thread::sleep(Duration::from_millis(10));
        client.cluster().events().serve();
    }
    assert_eq!(
        ctx.errors.lock().unwrap()[0],
        "Global error: Authentication (Local: Authentication failure): Failed to acquire SASL \
         OAUTHBEARER token: no credentials"
    );
}

/// A producer context recording failure logs and delivery results.
#[derive(Clone, Default)]
struct ProducerCtx {
    fails: Arc<Mutex<Vec<String>>>,
    delivered: Arc<Mutex<(u64, Vec<String>)>>,
}

impl ClientContext for ProducerCtx {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        if fac == "FAIL" && level != RDKafkaLogLevel::Debug {
            self.fails.lock().unwrap().push(log_message.to_string());
        }
    }

    fn resolve_broker_addr(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        let host = if host == "kafka" { "127.0.0.1" } else { host };
        (host, port).to_socket_addrs().map(|a| a.collect())
    }
}

impl crate::producer::ProducerContext for ProducerCtx {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &crate::message::DeliveryResult<'_>, _: ()) {
        let mut delivered = self.delivered.lock().unwrap();
        match result {
            Ok(_) => delivered.0 += 1,
            Err((e, _)) => delivered.1.push(e.to_string()),
        }
    }
}

/// A producer keeps producing across SASL session renewals: its sender moves
/// to the re-authenticated connection instead of being disconnected by the
/// broker when the session expires.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn producer_sasl_reauthentication() {
    use crate::producer::{BaseRecord, Producer, ThreadedProducer};
    let Some(_) = secrets() else { return };
    let sasl = [
        ("bootstrap.servers", "kafka:39095"),
        ("security.protocol", "sasl_plaintext"),
        ("sasl.mechanisms", "PLAIN"),
        ("sasl.username", "materialize"),
        ("sasl.password", "sekurity"),
    ];
    let topic = format!(
        "mzkc-reauth-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    {
        let (admin, _ctx) = client(&sasl).unwrap();
        super::tests::create_topic(admin.cluster(), &topic, 1);
    }
    let mut config = ClientConfig::new();
    config.set_log_level(RDKafkaLogLevel::Info);
    for (k, v) in sasl {
        config.set(k, v);
    }
    config.set("linger.ms", "0");
    let ctx = ProducerCtx::default();
    let producer: ThreadedProducer<ProducerCtx> = config.create_with_context(ctx.clone()).unwrap();
    let start = Instant::now();
    let mut sent = 0;
    // Sessions last 10 s: this spans two renewals.
    while start.elapsed() < Duration::from_secs(25) {
        producer
            .send(BaseRecord::<(), _, ()>::to(&topic).payload("x"))
            .map_err(|(e, _)| e)
            .unwrap();
        sent += 1;
        std::thread::sleep(Duration::from_millis(50));
    }
    producer.flush(Duration::from_secs(30)).unwrap();
    let (ok, errors) = ctx.delivered.lock().unwrap().clone();
    assert!(errors.is_empty(), "{errors:#?}");
    assert_eq!(ok, sent);
    let fails = ctx.fails.lock().unwrap().clone();
    assert!(fails.is_empty(), "{fails:#?}");
    let connects: i64 = producer
        .client()
        .cluster()
        .brokers()
        .iter()
        .map(|b| b.counters().connects.load(Ordering::Relaxed))
        .sum();
    assert!(connects >= 3, "{connects}");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn sasl_reauthentication() {
    let Some(_) = secrets() else { return };
    // The PLAIN session on 39095 lasts 10 s; requests keep working across the
    // renewal, which happens on a new connection.
    let (client, ctx) = client(&[
        ("bootstrap.servers", "kafka:39095"),
        ("security.protocol", "sasl_plaintext"),
        ("sasl.mechanisms", "PLAIN"),
        ("sasl.username", "materialize"),
        ("sasl.password", "sekurity"),
    ])
    .unwrap();
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(25) {
        client.fetch_metadata(None, Duration::from_secs(5)).unwrap();
        std::thread::sleep(Duration::from_millis(200));
    }
    let fails = ctx.fail_lines();
    assert!(fails.is_empty(), "{fails:#?}");
    let connects: i64 = client
        .cluster()
        .brokers()
        .iter()
        .map(|b| b.counters().connects.load(Ordering::Relaxed))
        .sum();
    assert!(connects >= 3, "{connects}");
}

/// A TCP proxy to `target` that can be stopped (closing all connections) and
/// restarted on the same port.
struct Proxy {
    port: u16,
    target: SocketAddr,
    stop: Arc<AtomicBool>,
}

impl Proxy {
    fn start(port: u16, target: SocketAddr) -> Proxy {
        let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut conns = vec![];
            while !stop2.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((client, _)) => {
                        let server = std::net::TcpStream::connect(target).unwrap();
                        for (mut a, mut b) in [
                            (client.try_clone().unwrap(), server.try_clone().unwrap()),
                            (server.try_clone().unwrap(), client.try_clone().unwrap()),
                        ] {
                            std::thread::spawn(move || {
                                let _ = io::copy(&mut a, &mut b);
                                let _ = b.shutdown(std::net::Shutdown::Both);
                            });
                        }
                        conns.push(client);
                        conns.push(server);
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(5)),
                }
            }
            for c in conns {
                let _ = c.shutdown(std::net::Shutdown::Both);
            }
        });
        Proxy { port, target, stop }
    }

    fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(50));
    }

    fn restart(&mut self) {
        *self = Proxy::start(self.port, self.target);
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn broker_restart_reconnects() {
    for brokers in super::tests::test_brokers() {
        let target = brokers
            .split(',')
            .next()
            .unwrap()
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        let mut proxy = Proxy::start(0, target);
        let (client, ctx) = client(&[("bootstrap.servers", "ignored:1")]).unwrap();
        // Every broker, bootstrap and advertised, goes through the proxy.
        ctx.redirect.store(proxy.port, Ordering::SeqCst);
        client
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
        client
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
        assert!(
            ctx.resolves
                .lock()
                .unwrap()
                .contains(&"ignored:1".to_string())
        );

        proxy.stop();
        let err = client
            .fetch_metadata(None, Duration::from_millis(500))
            .unwrap_err();
        assert_eq!(
            err,
            KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerTransportFailure)
        );
        let fails = ctx.fail_lines();
        assert!(
            fails.iter().any(|l| l
                .contains("Disconnected: connection closed by peer: receive 0 after POLLIN")
                || l.contains("Disconnected: connection reset by peer")),
            "{fails:#?}"
        );
        assert!(
            fails.iter().any(|l| l.contains("Connection refused")),
            "{fails:#?}"
        );

        proxy.restart();
        client
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
    }
}
