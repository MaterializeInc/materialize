// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for TLS encryption and authentication.

use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::fs::{self, File};
use std::io::Read;
use std::iter;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use headers::{Authorization, Header, HeaderMapExt};
use hyper::client::HttpConnector;
use hyper::http::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use hyper::http::uri::Scheme;
use hyper::service::{make_service_fn, service_fn};
use hyper::{body, Body, Request, Response, Server, StatusCode, Uri};
use hyper_openssl::HttpsConnector;
use jsonwebtoken::{self, EncodingKey};
use mz_ore::now::SYSTEM_TIME;
use openssl::asn1::Asn1Time;
use openssl::error::ErrorStack;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Private};
use openssl::rsa::Rsa;
use openssl::ssl::{
    SslConnector, SslConnectorBuilder, SslFiletype, SslMethod, SslOptions, SslVerifyMode,
};
use openssl::x509::extension::{BasicConstraints, SubjectAlternativeName};
use openssl::x509::{X509Name, X509NameBuilder, X509};
use postgres::config::SslMode;
use postgres::error::SqlState;
use postgres_openssl::MakeTlsConnector;
use serde::Deserialize;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use uuid::Uuid;

use materialized::TlsMode;
use mz_frontegg_auth::{
    ApiTokenArgs, ApiTokenResponse, Claims, FronteggAuthentication, FronteggConfig, RefreshToken,
    REFRESH_SUFFIX,
};
use mz_ore::assert_contains;
use mz_ore::now::NowFn;
use mz_ore::retry::Retry;
use mz_ore::task::RuntimeExt;

use crate::util::PostgresErrorExt;

pub mod util;

/// A certificate authority for use in tests.
pub struct Ca {
    dir: TempDir,
    name: X509Name,
    cert: X509,
    pkey: PKey<Private>,
}

impl Ca {
    fn make_ca(name: &str, parent: Option<&Ca>) -> Result<Ca, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;
        let rsa = Rsa::generate(2048)?;
        let pkey = PKey::from_rsa(rsa)?;
        let name = {
            let mut builder = X509NameBuilder::new()?;
            builder.append_entry_by_nid(Nid::COMMONNAME, name)?;
            builder.build()
        };
        let cert = {
            let mut builder = X509::builder()?;
            builder.set_version(2)?;
            builder.set_pubkey(&pkey)?;
            builder.set_issuer_name(parent.map(|ca| &ca.name).unwrap_or(&name))?;
            builder.set_subject_name(&name)?;
            builder.set_not_before(&*Asn1Time::days_from_now(0)?)?;
            builder.set_not_after(&*Asn1Time::days_from_now(365)?)?;
            builder.append_extension(BasicConstraints::new().critical().ca().build()?)?;
            builder.sign(
                parent.map(|ca| &ca.pkey).unwrap_or(&pkey),
                MessageDigest::sha256(),
            )?;
            builder.build()
        };
        fs::write(dir.path().join("ca.crt"), &cert.to_pem()?)?;
        Ok(Ca {
            dir,
            name,
            cert,
            pkey,
        })
    }

    /// Creates a new root certificate authority.
    pub fn new_root(name: &str) -> Result<Ca, Box<dyn Error>> {
        Ca::make_ca(name, None)
    }

    /// Returns the path to the CA's certificate.
    pub fn ca_cert_path(&self) -> PathBuf {
        self.dir.path().join("ca.crt")
    }

    /// Requests a new intermediate certificate authority.
    pub fn request_ca(&self, name: &str) -> Result<Ca, Box<dyn Error>> {
        Ca::make_ca(name, Some(self))
    }

    /// Generates a certificate with the specified Common Name (CN) that is
    /// signed by the CA.
    ///
    /// Returns the paths to the certificate and key.
    pub fn request_client_cert(&self, name: &str) -> Result<(PathBuf, PathBuf), Box<dyn Error>> {
        self.request_cert(name, iter::empty())
    }

    /// Like `request_client_cert`, but permits specifying additional IP
    /// addresses to attach as Subject Alternate Names.
    pub fn request_cert<I>(&self, name: &str, ips: I) -> Result<(PathBuf, PathBuf), Box<dyn Error>>
    where
        I: IntoIterator<Item = IpAddr>,
    {
        let rsa = Rsa::generate(2048)?;
        let pkey = PKey::from_rsa(rsa)?;
        let subject_name = {
            let mut builder = X509NameBuilder::new()?;
            builder.append_entry_by_nid(Nid::COMMONNAME, name)?;
            builder.build()
        };
        let cert = {
            let mut builder = X509::builder()?;
            builder.set_version(2)?;
            builder.set_pubkey(&pkey)?;
            builder.set_issuer_name(&self.cert.subject_name())?;
            builder.set_subject_name(&subject_name)?;
            builder.set_not_before(&*Asn1Time::days_from_now(0)?)?;
            builder.set_not_after(&*Asn1Time::days_from_now(365)?)?;
            for ip in ips {
                builder.append_extension(
                    SubjectAlternativeName::new()
                        .ip(&ip.to_string())
                        .build(&builder.x509v3_context(None, None))?,
                )?;
            }
            builder.sign(&self.pkey, MessageDigest::sha256())?;
            builder.build()
        };
        let cert_path = self.dir.path().join(Path::new(name).with_extension("crt"));
        let key_path = self.dir.path().join(Path::new(name).with_extension("key"));
        fs::write(&cert_path, &cert.to_pem()?)?;
        fs::write(&key_path, &pkey.private_key_to_pem_pkcs8()?)?;
        Ok((cert_path, key_path))
    }
}

fn make_pg_tls<F>(configure: F) -> MakeTlsConnector
where
    F: Fn(&mut SslConnectorBuilder) -> Result<(), ErrorStack>,
{
    let mut connector_builder = SslConnector::builder(SslMethod::tls()).unwrap();
    // Disable TLS v1.3 because `postgres` and `hyper` produce stabler error
    // messages with TLS v1.2.
    //
    // Briefly, in TLS v1.3, failing to present a client certificate does not
    // error during the TLS handshake, as it does in TLS v1.2, but on the first
    // attempt to read from the stream. But both `postgres` and `hyper` write a
    // bunch of data before attempting to read from the stream. With a failed
    // TLS v1.3 connection, sometimes `postgres` and `hyper` succeed in writing
    // out this data, and then return a nice error message on the call to read.
    // But sometimes the connection is closed before they write out the data,
    // and so they report "connection closed" before they ever call read, never
    // noticing the underlying SSL error.
    //
    // It's unclear who's bug this is. Is it on `hyper`/`postgres` to call read
    // if writing to the stream fails to see if a TLS error occured? Is it on
    // OpenSSL to provide a better API [1]? Is it a protocol issue that ought to
    // be corrected in TLS v1.4? We don't want to answer these questions, so we
    // just avoid TLS v1.3 for now.
    //
    // [1]: https://github.com/openssl/openssl/issues/11118
    let options = connector_builder.options() | SslOptions::NO_TLSV1_3;
    connector_builder.set_options(options);
    configure(&mut connector_builder).unwrap();
    MakeTlsConnector::new(connector_builder.build())
}

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

enum Assert<E> {
    Success,
    Err(E),
}

enum TestCase<'a> {
    Pgwire {
        user: &'static str,
        password: Option<&'a str>,
        ssl_mode: SslMode,
        configure: Box<dyn Fn(&mut SslConnectorBuilder) -> Result<(), ErrorStack> + 'a>,
        assert: Assert<Box<dyn Fn(postgres::Error) + 'a>>,
    },
    Http {
        user: &'static str,
        scheme: Scheme,
        headers: &'a HeaderMap,
        configure: Box<dyn Fn(&mut SslConnectorBuilder) -> Result<(), ErrorStack> + 'a>,
        assert: Assert<Box<dyn Fn(Option<StatusCode>, String) + 'a>>,
    },
}

fn run_tests<'a>(header: &str, server: &util::Server, tests: &[TestCase<'a>]) {
    println!("==> {}", header);
    let runtime = Runtime::new().unwrap();
    for test in tests {
        match test {
            TestCase::Pgwire {
                user,
                password,
                ssl_mode,
                configure,
                assert,
            } => {
                println!("pgwire user={} ssl_mode={:?}", user, ssl_mode);

                let pg_client = server
                    .pg_config()
                    .ssl_mode(*ssl_mode)
                    .user(user)
                    .password(password.unwrap_or(""))
                    .connect(make_pg_tls(configure));

                match assert {
                    Assert::Success => {
                        let mut pg_client = pg_client.unwrap();
                        let row = pg_client.query_one("SELECT current_user", &[]).unwrap();
                        assert_eq!(row.get::<_, String>(0), *user);
                    }
                    Assert::Err(check) => check(pg_client.err().unwrap()),
                }
            }
            TestCase::Http {
                user,
                scheme,
                headers,
                configure,
                assert,
            } => {
                println!("http user={} scheme={}", user, scheme);

                let uri = Uri::builder()
                    .scheme(scheme.clone())
                    .authority(&*format!(
                        "{}:{}",
                        Ipv4Addr::LOCALHOST,
                        server.inner.local_addr().port()
                    ))
                    .path_and_query("/sql")
                    .build()
                    .unwrap();
                let res = runtime.block_on(
                    hyper::Client::builder()
                        .build::<_, Body>(make_http_tls(configure))
                        .request({
                            let mut req = Request::post(uri);
                            for (k, v) in headers.iter() {
                                req.headers_mut().unwrap().insert(k, v.clone());
                            }
                            req.body(Body::from("sql=SELECT pg_catalog.current_user()"))
                                .unwrap()
                        }),
                );
                match assert {
                    Assert::Success => {
                        #[derive(Deserialize)]
                        struct Result {
                            rows: Vec<Vec<String>>,
                        }
                        #[derive(Deserialize)]
                        struct Response {
                            results: Vec<Result>,
                        }
                        let body = runtime
                            .block_on(body::to_bytes(res.unwrap().into_body()))
                            .unwrap();
                        let res: Response = serde_json::from_slice(&body).unwrap();
                        assert_eq!(res.results[0].rows, vec![vec![user.to_string()]])
                    }
                    Assert::Err(check) => {
                        let (code, message) = match res {
                            Ok(mut res) => {
                                let body = String::from_utf8_lossy(
                                    &runtime.block_on(body::to_bytes(res.body_mut())).unwrap(),
                                )
                                .into_owned();
                                (Some(res.status()), body)
                            }
                            Err(e) => (None, e.to_string()),
                        };
                        check(code, message)
                    }
                }
            }
        }
    }
}

// Users is a mapping from (client, secret) -> email address.
fn start_mzcloud(
    encoding_key: EncodingKey,
    tenant_id: Uuid,
    users: HashMap<(String, String), String>,
    now: NowFn,
    expires_in_secs: i64,
) -> Result<MzCloudServer, anyhow::Error> {
    let refreshes = Arc::new(Mutex::new(0u64));
    let enable_refresh = Arc::new(AtomicBool::new(true));
    #[derive(Clone)]
    struct Context {
        encoding_key: EncodingKey,
        tenant_id: Uuid,
        users: HashMap<(String, String), String>,
        now: NowFn,
        expires_in_secs: i64,
        // Uuid -> email
        refresh_tokens: Arc<Mutex<HashMap<String, String>>>,
        refreshes: Arc<Mutex<u64>>,
        enable_refresh: Arc<AtomicBool>,
    }
    let context = Context {
        encoding_key,
        tenant_id,
        users,
        now,
        expires_in_secs,
        refresh_tokens: Arc::new(Mutex::new(HashMap::new())),
        refreshes: Arc::clone(&refreshes),
        enable_refresh: Arc::clone(&enable_refresh),
    };
    async fn handle(context: Context, req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let (parts, body) = req.into_parts();
        let body = body::to_bytes(body).await.unwrap();
        let email: String = if parts.uri.path().ends_with(REFRESH_SUFFIX) {
            // Always count refresh attempts, even if enable_refresh is false.
            *context.refreshes.lock().unwrap() += 1;
            let args: RefreshToken = serde_json::from_slice(&body).unwrap();
            match (
                context
                    .refresh_tokens
                    .lock()
                    .unwrap()
                    .get(&args.refresh_token),
                context.enable_refresh.load(Ordering::Relaxed),
            ) {
                (Some(email), true) => email.to_string(),
                _ => {
                    return Ok(Response::builder()
                        .status(400)
                        .body(Body::from("unknown refresh token"))
                        .unwrap())
                }
            }
        } else {
            let args: ApiTokenArgs = serde_json::from_slice(&body).unwrap();
            match context
                .users
                .get(&(args.client_id.to_string(), args.secret.to_string()))
            {
                Some(email) => email.to_string(),
                None => {
                    return Ok(Response::builder()
                        .status(400)
                        .body(Body::from("unknown user"))
                        .unwrap())
                }
            }
        };
        let refresh_token = Uuid::new_v4().to_string();
        context
            .refresh_tokens
            .lock()
            .unwrap()
            .insert(refresh_token.clone(), email.clone());
        let access_token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &Claims {
                exp: context.now.as_secs() + context.expires_in_secs,
                email,
                tenant_id: context.tenant_id,
                roles: Vec::new(),
                permissions: Vec::new(),
            },
            &context.encoding_key,
        )
        .unwrap();
        let resp = ApiTokenResponse {
            expires: "".to_string(),
            expires_in: 0,
            access_token,
            refresh_token,
        };
        Ok(Response::new(Body::from(
            serde_json::to_vec(&resp).unwrap(),
        )))
    }

    let runtime = Arc::new(Runtime::new()?);
    let _guard = runtime.enter();
    let service = make_service_fn(move |_conn| {
        let context = context.clone();
        let service = service_fn(move |req| handle(context.clone(), req));
        async move { Ok::<_, Infallible>(service) }
    });
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = Server::bind(&addr).serve(service);
    let url = format!("http://{}/", server.local_addr());
    let _handle = runtime.spawn_named(|| "mzcloud-mock-server", server);
    Ok(MzCloudServer {
        url,
        refreshes,
        enable_refresh,
        _runtime: runtime,
    })
}

struct MzCloudServer {
    url: String,
    refreshes: Arc<Mutex<u64>>,
    enable_refresh: Arc<AtomicBool>,
    _runtime: Arc<Runtime>,
}

fn make_header<H: Header>(h: H) -> HeaderMap {
    let mut map = HeaderMap::new();
    map.typed_insert(h);
    map
}

#[test]
fn test_auth_expiry() -> Result<(), Box<dyn Error>> {
    // This function verifies that the background expiry refresh task runs. This
    // is done by starting a web server that awaits the refresh request, which the
    // test waits for.

    mz_ore::test::init_logging();

    let ca = Ca::new_root("test ca")?;
    let (server_cert, server_key) =
        ca.request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])?;

    let tenant_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let users = HashMap::from([(
        (client_id.to_string(), secret.to_string()),
        "user@_.com".to_string(),
    )]);
    let encoding_key = EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap())?;

    const EXPIRES_IN_SECS: u64 = 2;
    const REFRESH_BEFORE_SECS: u64 = 1;
    let frontegg_server = start_mzcloud(
        encoding_key,
        tenant_id,
        users,
        SYSTEM_TIME.clone(),
        EXPIRES_IN_SECS as i64,
    )?;
    let frontegg_auth = FronteggAuthentication::new(FronteggConfig {
        admin_api_token_url: frontegg_server.url.clone(),
        jwk_rsa_pem: &ca.pkey.public_key_to_pem()?,
        tenant_id,
        now: SYSTEM_TIME.clone(),
        refresh_before_secs: REFRESH_BEFORE_SECS as i64,
    })?;
    let frontegg_user = "user@_.com";
    let frontegg_password = &format!("{client_id}{secret}");

    let wait_for_refresh = || {
        let expected = *frontegg_server.refreshes.lock().unwrap() + 1;
        Retry::default()
            .factor(1.0)
            .max_duration(Duration::from_secs(EXPIRES_IN_SECS + 1))
            .retry(|_| {
                let refreshes = *frontegg_server.refreshes.lock().unwrap();
                if refreshes == expected {
                    Ok(())
                } else {
                    Err(format!(
                        "expected refresh count {}, got {}",
                        expected, refreshes
                    ))
                }
            })
            .unwrap();
    };

    let config = util::Config::default()
        .with_tls(TlsMode::Require, &server_cert, &server_key)
        .with_frontegg(&frontegg_auth);
    let server = util::start_server(config)?;

    let mut pg_client = server
        .pg_config()
        .ssl_mode(SslMode::Require)
        .user(frontegg_user)
        .password(frontegg_password)
        .connect(make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        })))
        .unwrap();

    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .unwrap()
            .get::<_, String>(0),
        frontegg_user
    );

    // Wait for a couple refreshes to happen.
    wait_for_refresh();
    wait_for_refresh();
    assert_eq!(
        pg_client
            .query_one("SELECT current_user", &[])
            .unwrap()
            .get::<_, String>(0),
        frontegg_user
    );

    // Disable giving out more refresh tokens.
    frontegg_server
        .enable_refresh
        .store(false, Ordering::Relaxed);
    wait_for_refresh();
    // Sleep until the expiry future should resolve.
    std::thread::sleep(Duration::from_secs(EXPIRES_IN_SECS - REFRESH_BEFORE_SECS));
    assert!(pg_client.query_one("SELECT current_user", &[]).is_err());

    Ok(())
}

#[allow(clippy::unit_arg)]
#[test]
fn test_auth() -> Result<(), Box<dyn Error>> {
    mz_ore::test::init_logging();

    let ca = Ca::new_root("test ca")?;
    let (server_cert, server_key) =
        ca.request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])?;
    let (client_cert, client_key) = ca.request_client_cert("materialize")?;
    let (client_cert_other, client_key_other) = ca.request_client_cert("other")?;
    let (client_cert_cloud, client_key_cloud) = ca.request_client_cert("user@_.com")?;

    let bad_ca = Ca::new_root("test ca")?;
    let (bad_client_cert, bad_client_key) = bad_ca.request_client_cert("materialize")?;

    let tenant_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let users = HashMap::from([(
        (client_id.to_string(), secret.to_string()),
        "user@_.com".to_string(),
    )]);
    let encoding_key = EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap())?;
    let timestamp = Arc::new(Mutex::new(500_000));
    let now = {
        let timestamp = Arc::clone(&timestamp);
        NowFn::from(move || *timestamp.lock().unwrap())
    };
    let claims = Claims {
        exp: 1000,
        email: "user@_.com".to_string(),
        tenant_id,
        roles: Vec::new(),
        permissions: Vec::new(),
    };
    let frontegg_jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        &encoding_key,
    )
    .unwrap();
    let bad_tenant_claims = {
        let mut claims = claims.clone();
        claims.tenant_id = Uuid::new_v4();
        claims
    };
    let bad_tenant_jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &bad_tenant_claims,
        &encoding_key,
    )
    .unwrap();
    let expired_claims = {
        let mut claims = claims;
        claims.exp = 0;
        claims
    };
    let expired_jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &expired_claims,
        &encoding_key,
    )
    .unwrap();
    let frontegg_server = start_mzcloud(encoding_key, tenant_id, users, now.clone(), 1_000)?;
    let frontegg_auth = FronteggAuthentication::new(FronteggConfig {
        admin_api_token_url: frontegg_server.url,
        jwk_rsa_pem: &ca.pkey.public_key_to_pem()?,
        tenant_id,
        now,
        refresh_before_secs: 0,
    })?;
    let frontegg_user = "user@_.com";
    let frontegg_password = &format!("{client_id}{secret}");
    let frontegg_basic = Authorization::basic(frontegg_user, frontegg_password);
    let frontegg_header_basic = make_header(frontegg_basic);

    let no_headers = HeaderMap::new();

    // Test connecting to a server that requires client TLS and uses Materialize
    // Cloud for authentication.
    let config = util::Config::default()
        .with_tls(
            TlsMode::VerifyFull {
                ca: ca.ca_cert_path(),
            },
            &server_cert,
            &server_key,
        )
        .with_frontegg(&frontegg_auth);
    let server = util::start_server(config)?;
    run_tests(
        "TlsMode::VerifyFull, MzCloud",
        &server,
        &[
            // Succeed if the cert user matches the JWT's email.
            TestCase::Pgwire {
                user: frontegg_user,
                password: Some(frontegg_password),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| {
                    b.set_ca_file(ca.ca_cert_path())?;
                    b.set_certificate_file(&client_cert_cloud, SslFiletype::PEM)?;
                    b.set_private_key_file(&client_key_cloud, SslFiletype::PEM)
                }),
                assert: Assert::Success,
            },
            TestCase::Http {
                user: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &frontegg_header_basic,
                configure: Box::new(|b| {
                    b.set_ca_file(ca.ca_cert_path())?;
                    b.set_certificate_file(&client_cert_cloud, SslFiletype::PEM)?;
                    b.set_private_key_file(&client_key_cloud, SslFiletype::PEM)
                }),
                assert: Assert::Success,
            },
            // Fail if the cert user doesn't match the JWT's email.
            TestCase::Pgwire {
                user: "materialize",
                password: Some(frontegg_password),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| {
                    b.set_ca_file(ca.ca_cert_path())?;
                    b.set_certificate_file(&client_cert, SslFiletype::PEM)?;
                    b.set_private_key_file(&client_key, SslFiletype::PEM)
                }),
                assert: Assert::Err(Box::new(|err| {
                    assert_contains!(err.to_string(), "invalid password");
                })),
            },
            TestCase::Http {
                user: "materialize",
                scheme: Scheme::HTTPS,
                headers: &frontegg_header_basic,
                configure: Box::new(|b| {
                    b.set_ca_file(ca.ca_cert_path())?;
                    b.set_certificate_file(&client_cert, SslFiletype::PEM)?;
                    b.set_private_key_file(&client_key, SslFiletype::PEM)
                }),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_contains!(message, "unauthorized");
                })),
            },
        ],
    );

    // Test connecting to a server that requires TLS and uses Materialize Cloud for
    // authentication.
    let config = util::Config::default()
        .with_tls(TlsMode::Require, &server_cert, &server_key)
        .with_frontegg(&frontegg_auth);
    let server = util::start_server(config)?;
    run_tests(
        "TlsMode::Require, MzCloud",
        &server,
        &[
            // TLS with a password should succeed.
            TestCase::Pgwire {
                user: frontegg_user,
                password: Some(frontegg_password),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            TestCase::Http {
                user: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &frontegg_header_basic,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Password can be base64 encoded UUID bytes.
            TestCase::Pgwire {
                user: frontegg_user,
                password: {
                    let mut buf = vec![];
                    buf.extend(client_id.as_bytes());
                    buf.extend(secret.as_bytes());
                    Some(&base64::encode_config(buf, base64::URL_SAFE))
                },
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Password can be base64 encoded UUID bytes without padding.
            TestCase::Pgwire {
                user: frontegg_user,
                password: {
                    let mut buf = vec![];
                    buf.extend(client_id.as_bytes());
                    buf.extend(secret.as_bytes());
                    Some(&base64::encode_config(buf, base64::URL_SAFE_NO_PAD))
                },
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Password can include arbitrary special characters.
            TestCase::Pgwire {
                user: frontegg_user,
                password: {
                    let mut password = frontegg_password.clone();
                    password.insert(3, '-');
                    password.insert_str(12, "@#!");
                    Some(&password.clone())
                },
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Bearer auth doesn't need the clientid or secret.
            TestCase::Http {
                user: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::bearer(&frontegg_jwt).unwrap()),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // No TLS fails.
            TestCase::Pgwire {
                user: frontegg_user,
                password: Some(frontegg_password),
                ssl_mode: SslMode::Disable,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|err| {
                    let err = err.unwrap_db_error();
                    assert_eq!(
                        *err.code(),
                        SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
                    );
                    assert_eq!(err.message(), "TLS encryption is required");
                })),
            },
            TestCase::Http {
                user: frontegg_user,
                scheme: Scheme::HTTP,
                headers: &frontegg_header_basic,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "HTTPS is required");
                })),
            },
            // Wrong, but existing, username.
            TestCase::Pgwire {
                user: "materialize",
                password: Some(frontegg_password),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|err| {
                    let err = err.unwrap_db_error();
                    assert_eq!(err.message(), "invalid password");
                    assert_eq!(*err.code(), SqlState::INVALID_PASSWORD);
                })),
            },
            TestCase::Http {
                user: "materialize",
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
                user: frontegg_user,
                password: Some("bad password"),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|err| {
                    let err = err.unwrap_db_error();
                    assert_eq!(err.message(), "invalid password");
                    assert_eq!(*err.code(), SqlState::INVALID_PASSWORD);
                })),
            },
            TestCase::Http {
                user: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::basic(frontegg_user, "bad password")),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "unauthorized");
                })),
            },
            // No password.
            TestCase::Pgwire {
                user: frontegg_user,
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|err| {
                    let err = err.unwrap_db_error();
                    assert_eq!(err.message(), "invalid password");
                    assert_eq!(*err.code(), SqlState::INVALID_PASSWORD);
                })),
            },
            TestCase::Http {
                user: frontegg_user,
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
                user: frontegg_user,
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
                user: frontegg_user,
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
                user: frontegg_user,
                scheme: Scheme::HTTPS,
                headers: &make_header(Authorization::bearer(&expired_jwt).unwrap()),
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "unauthorized");
                })),
            },
        ],
    );

    // Test TLS modes with a server that does not support TLS.
    let server = util::start_server(util::Config::default())?;
    run_tests(
        "TlsMode::Disable",
        &server,
        &[
            // Explicitly disabling TLS should succeed.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Disable,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Success,
            },
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTP,
                headers: &no_headers,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Success,
            },
            // Preferring TLS should fall back to no TLS.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Prefer,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Success,
            },
            // Requiring TLS should fail.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Err(Box::new(|err| {
                    assert_eq!(
                        err.to_string(),
                        "error performing TLS handshake: server does not support TLS",
                    )
                })),
            },
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Err(Box::new(|code, message| {
                    // Connecting to an HTTP server via HTTPS does not yield
                    // a graceful error message. This could plausibly change
                    // due to OpenSSL or Hyper refactorings.
                    assert!(code.is_none());
                    assert_contains!(message, "ssl3_get_record:wrong version number");
                })),
            },
        ],
    );

    // Test TLS modes with a server that requires TLS.
    let config = util::Config::default().with_tls(TlsMode::Require, &server_cert, &server_key);
    let server = util::start_server(config)?;
    run_tests(
        "TlsMode::Require",
        &server,
        &[
            // Mz Cloud auth should fail.
            TestCase::Pgwire {
                user: frontegg_user,
                password: Some(frontegg_password),
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|err| {
                    let err = err.unwrap_db_error();
                    assert_eq!(err.message(), r#"role "user@_.com" does not exist"#);
                    assert_eq!(*err.code(), SqlState::INVALID_AUTHORIZATION_SPECIFICATION);
                })),
            },
            // Test that specifying an mzcloud header does nothing and uses the default
            // user.
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTPS,
                headers: &frontegg_header_basic,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Disabling TLS should fail.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Disable,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Err(Box::new(|err| {
                    let err = err.unwrap_db_error();
                    assert_eq!(
                        *err.code(),
                        SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
                    );
                    assert_eq!(err.message(), "TLS encryption is required");
                })),
            },
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTP,
                headers: &no_headers,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "HTTPS is required");
                })),
            },
            // Preferring TLS should succeed.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Prefer,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            // Requiring TLS should succeed.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Success,
            },
        ],
    );

    // Test connecting to a server that verifies client certificates.
    let config = util::Config::default().with_tls(
        TlsMode::VerifyCa {
            ca: ca.ca_cert_path(),
        },
        &server_cert,
        &server_key,
    );
    let server = util::start_server(config)?;
    server
        .connect(make_pg_tls(|b| {
            b.set_ca_file(ca.ca_cert_path())?;
            b.set_certificate_file(&client_cert, SslFiletype::PEM)?;
            b.set_private_key_file(&client_key, SslFiletype::PEM)
        }))?
        .batch_execute("CREATE ROLE other LOGIN SUPERUSER")?;
    run_tests(
        "TlsMode::VerifyCa",
        &server,
        &[
            // Disabling TLS should fail.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Disable,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Err(Box::new(|err| {
                    let err = err.unwrap_db_error();
                    assert_eq!(
                        *err.code(),
                        SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
                    );
                    assert_eq!(err.message(), "TLS encryption is required");
                })),
            },
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTP,
                headers: &no_headers,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "HTTPS is required");
                })),
            },
            // Connecting with TLS without providing a client certificate should
            // fail.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|err| {
                    assert_contains!(
                        err.to_string(),
                        "self signed certificate in certificate chain"
                    )
                })),
            },
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert!(code.is_none());
                    assert_contains!(message, "self signed certificate in certificate chain")
                })),
            },
            // Connecting with TLS with a bad client certificate should fail.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| {
                    b.set_ca_file(bad_ca.ca_cert_path())?;
                    b.set_certificate_file(&bad_client_cert, SslFiletype::PEM)?;
                    b.set_private_key_file(&bad_client_key, SslFiletype::PEM)
                }),
                assert: Assert::Err(Box::new(|err| {
                    assert_contains!(err.to_string(), "certificate signature failure")
                })),
            },
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|b| {
                    b.set_ca_file(bad_ca.ca_cert_path())?;
                    b.set_certificate_file(&bad_client_cert, SslFiletype::PEM)?;
                    b.set_private_key_file(&bad_client_key, SslFiletype::PEM)
                }),
                assert: Assert::Err(Box::new(|code, message| {
                    assert!(code.is_none());
                    assert_contains!(message, "certificate signature failure");
                })),
            },
            // Connecting with a valid client certificate should succeed.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| {
                    b.set_ca_file(ca.ca_cert_path())?;
                    b.set_certificate_file(&client_cert, SslFiletype::PEM)?;
                    b.set_private_key_file(&client_key, SslFiletype::PEM)
                }),
                assert: Assert::Success,
            },
            TestCase::Http {
                // In verify-ca mode, the HTTP interface ignores the
                // certificate's user.
                user: "mz_system",
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|b| {
                    b.set_ca_file(ca.ca_cert_path())?;
                    b.set_certificate_file(&client_cert, SslFiletype::PEM)?;
                    b.set_private_key_file(&client_key, SslFiletype::PEM)
                }),
                assert: Assert::Success,
            },
            // Connecting with a valid client certificate should succeed even if
            // connecting to a user with a different name than the certificate's
            // CN.
            TestCase::Pgwire {
                user: "other",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| {
                    b.set_ca_file(ca.ca_cert_path())?;
                    b.set_certificate_file(&client_cert, SslFiletype::PEM)?;
                    b.set_private_key_file(&client_key, SslFiletype::PEM)
                }),
                assert: Assert::Success,
            },
        ],
    );

    // Test connecting to a server that both verifies client certificates and
    // verifies that the CN matches the pgwire user name.
    let config = util::Config::default().with_tls(
        TlsMode::VerifyFull {
            ca: ca.ca_cert_path(),
        },
        &server_cert,
        &server_key,
    );
    let server = util::start_server(config)?;
    server
        .connect(make_pg_tls(|b| {
            b.set_ca_file(ca.ca_cert_path())?;
            b.set_certificate_file(&client_cert, SslFiletype::PEM)?;
            b.set_private_key_file(&client_key, SslFiletype::PEM)
        }))?
        .batch_execute("CREATE ROLE other LOGIN SUPERUSER")?;
    run_tests(
        "TlsMode::VerifyFull",
        &server,
        &[
            // Disabling TLS should fail.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Disable,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Err(Box::new(|err| {
                    let err = err.unwrap_db_error();
                    assert_eq!(
                        *err.code(),
                        SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
                    );
                    assert_eq!(err.message(), "TLS encryption is required");
                })),
            },
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTP,
                headers: &no_headers,
                configure: Box::new(|_| Ok(())),
                assert: Assert::Err(Box::new(|code, message| {
                    assert_eq!(code, Some(StatusCode::UNAUTHORIZED));
                    assert_eq!(message, "HTTPS is required");
                })),
            },
            // Connecting with TLS without providing a client certificate should
            // fail.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|err| {
                    assert_contains!(
                        err.to_string(),
                        "self signed certificate in certificate chain"
                    )
                })),
            },
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|b| Ok(b.set_verify(SslVerifyMode::NONE))),
                assert: Assert::Err(Box::new(|code, message| {
                    assert!(code.is_none());
                    assert_contains!(message, "self signed certificate in certificate chain")
                })),
            },
            // Connecting with TLS with a bad client certificate should fail.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| {
                    b.set_ca_file(bad_ca.ca_cert_path())?;
                    b.set_certificate_file(&bad_client_cert, SslFiletype::PEM)?;
                    b.set_private_key_file(&bad_client_key, SslFiletype::PEM)
                }),
                assert: Assert::Err(Box::new(|err| {
                    assert_contains!(err.to_string(), "certificate signature failure");
                })),
            },
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|b| {
                    b.set_ca_file(bad_ca.ca_cert_path())?;
                    b.set_certificate_file(&bad_client_cert, SslFiletype::PEM)?;
                    b.set_private_key_file(&bad_client_key, SslFiletype::PEM)
                }),
                assert: Assert::Err(Box::new(|code, message| {
                    assert!(code.is_none());
                    assert_contains!(message, "certificate signature failure");
                })),
            },
            // Connecting with a valid client certificate should succeed.
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| {
                    b.set_ca_file(ca.ca_cert_path())?;
                    b.set_certificate_file(&client_cert, SslFiletype::PEM)?;
                    b.set_private_key_file(&client_key, SslFiletype::PEM)
                }),
                assert: Assert::Success,
            },
            TestCase::Http {
                user: "materialize",
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|b| {
                    b.set_ca_file(ca.ca_cert_path())?;
                    b.set_certificate_file(&client_cert, SslFiletype::PEM)?;
                    b.set_private_key_file(&client_key, SslFiletype::PEM)
                }),
                assert: Assert::Success,
            },
            // Connecting with a valid client certificate should fail if
            // connecting to a user with a different name than the certificate's
            // CN.
            TestCase::Pgwire {
                user: "other",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| {
                    b.set_ca_file(ca.ca_cert_path())?;
                    b.set_certificate_file(&client_cert, SslFiletype::PEM)?;
                    b.set_private_key_file(&client_key, SslFiletype::PEM)
                }),
                assert: Assert::Err(Box::new(|err| {
                    let err = err.unwrap_db_error();
                    assert_eq!(*err.code(), SqlState::INVALID_AUTHORIZATION_SPECIFICATION);
                    assert_eq!(
                        err.message(),
                        "certificate authentication failed for user \"other\""
                    );
                })),
            },
            // But it should succeed if connecting to that user with the
            // appropriate certificate.
            TestCase::Pgwire {
                user: "other",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| {
                    b.set_ca_file(ca.ca_cert_path())?;
                    b.set_certificate_file(&client_cert_other, SslFiletype::PEM)?;
                    b.set_private_key_file(&client_key_other, SslFiletype::PEM)
                }),
                assert: Assert::Success,
            },
            TestCase::Http {
                user: "other",
                scheme: Scheme::HTTPS,
                headers: &no_headers,
                configure: Box::new(|b| {
                    b.set_ca_file(ca.ca_cert_path())?;
                    b.set_certificate_file(&client_cert_other, SslFiletype::PEM)?;
                    b.set_private_key_file(&client_key_other, SslFiletype::PEM)
                }),
                assert: Assert::Success,
            },
        ],
    );

    Ok(())
}

#[test]
fn test_auth_intermediate_ca() -> Result<(), Box<dyn Error>> {
    // Create a CA, an intermediate CA, and a server key pair signed by the
    // intermediate CA.
    let ca = Ca::new_root("test ca")?;
    let intermediate_ca = ca.request_ca("intermediary")?;
    let (server_cert, server_key) =
        intermediate_ca.request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])?;

    // Create a certificate chain bundle that contains the server's certificate
    // and the intermediate CA's certificate.
    let server_cert_chain = {
        let path = intermediate_ca.dir.path().join("server.chain.crt");
        let mut buf = vec![];
        File::open(&server_cert)?.read_to_end(&mut buf)?;
        File::open(intermediate_ca.ca_cert_path())?.read_to_end(&mut buf)?;
        fs::write(&path, buf)?;
        path
    };

    // When the server presents only its own certificate, without the
    // intermediary, the client should fail to verify the chain.
    let config = util::Config::default().with_tls(TlsMode::Require, &server_cert, &server_key);
    let server = util::start_server(config)?;
    run_tests(
        "TlsMode::Require",
        &server,
        &[
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| b.set_ca_file(ca.ca_cert_path())),
                assert: Assert::Err(Box::new(|err| {
                    assert_contains!(err.to_string(), "unable to get local issuer certificate");
                })),
            },
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTPS,
                headers: &HeaderMap::new(),
                configure: Box::new(|b| b.set_ca_file(ca.ca_cert_path())),
                assert: Assert::Err(Box::new(|code, message| {
                    assert!(code.is_none());
                    assert_contains!(message, "unable to get local issuer certificate");
                })),
            },
        ],
    );

    // When the server is configured to present the entire certificate chain,
    // the client should be able to verify the chain even though it only knows
    // about the root CA.
    let config =
        util::Config::default().with_tls(TlsMode::Require, &server_cert_chain, &server_key);
    let server = util::start_server(config)?;
    run_tests(
        "TlsMode::Require",
        &server,
        &[
            TestCase::Pgwire {
                user: "materialize",
                password: None,
                ssl_mode: SslMode::Require,
                configure: Box::new(|b| b.set_ca_file(ca.ca_cert_path())),
                assert: Assert::Success,
            },
            TestCase::Http {
                user: "mz_system",
                scheme: Scheme::HTTPS,
                headers: &HeaderMap::new(),
                configure: Box::new(|b| b.set_ca_file(ca.ca_cert_path())),
                assert: Assert::Success,
            },
        ],
    );

    Ok(())
}
