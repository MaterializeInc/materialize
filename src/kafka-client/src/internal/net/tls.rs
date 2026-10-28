// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TLS with OpenSSL, configured like librdkafka's `rdkafka_ssl.c`.
//!
//! [`TlsConfig::new`] builds the `SSL_CTX` once per client, so malformed PEM,
//! unreadable keystores and mismatched keys fail client creation with
//! librdkafka's texts, e.g. `ssl.key.pem failed: not in PEM format?: <openssl
//! error>`. Every connection then creates an `SSL` from it with SNI set to
//! the broker's configured or advertised host name, which is also the name
//! verified against the certificate when
//! `ssl.endpoint.identification.algorithm=https`, regardless of the address
//! the resolve hook returned.

use std::fmt::Write;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use openssl::error::ErrorStack;
use openssl::pkcs12::Pkcs12;
use openssl::pkey::{PKey, Private};
use openssl::ssl::{
    Ssl, SslContext, SslContextBuilder, SslFiletype, SslMethod, SslOptions, SslVerifyMode,
};
use openssl::x509::{X509, X509VerifyResult};
use tokio::net::TcpStream;
use tokio_openssl::SslStream;

use crate::config::RDKafkaLogLevel;
use crate::config::settings::SslSettings;
use crate::internal::hooks::Logger;
use crate::types::RDKafkaErrorCode;

use super::Failure;

/// librdkafka's CA bundle probe list, used when no CA is configured, since
/// the vendored OpenSSL's compiled-in default paths do not match the system.
const CA_PROBE_PATHS: &[&str] = &[
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/certs/ca-bundle.crt",
    "/etc/pki/tls/certs/ca-bundle.trust.crt",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/ssl/ca-bundle.pem",
    "/etc/pki/tls/cacert.pem",
    "/etc/ssl/cert.pem",
    "/etc/ssl/cacert.pem",
    "/etc/certs/ca-certificates.crt",
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/ssl/certs",
    "/usr/local/etc/ssl/cert.pem",
    "/usr/local/etc/ssl/cacert.pem",
    "/usr/local/etc/ssl/certs/cert.pem",
    "/usr/local/etc/ssl/certs/cacert.pem",
    "/usr/local/share/certs/ca-root-nss.crt",
    "/etc/openssl/certs/ca-certificates.crt",
    #[cfg(target_os = "macos")]
    "/private/etc/ssl/cert.pem",
    #[cfg(target_os = "macos")]
    "/private/etc/ssl/certs",
    #[cfg(target_os = "macos")]
    "/usr/local/etc/openssl@1.1/cert.pem",
    #[cfg(target_os = "macos")]
    "/usr/local/etc/openssl@1.0/cert.pem",
    #[cfg(target_os = "macos")]
    "/usr/local/etc/openssl/certs",
    #[cfg(target_os = "macos")]
    "/System/Library/OpenSSL",
];

/// The TLS client configuration of one client.
#[derive(Clone)]
pub(crate) struct TlsConfig {
    ctx: Arc<SslContext>,
    endpoint_identification: bool,
    enable_verification: bool,
}

impl TlsConfig {
    /// Builds the TLS context. Fails with librdkafka's client creation text.
    ///
    /// OpenSSL errors that precede the final one are logged with facility
    /// `SSL`, as librdkafka does. The thread's OpenSSL error queue is empty
    /// afterwards, also on success.
    pub(crate) fn new(ssl: &SslSettings, logger: &Logger) -> Result<TlsConfig, String> {
        let result = build_context(ssl, logger);
        // Leave no stale errors behind for unrelated OpenSSL users on this
        // thread.
        let _ = ErrorStack::get();
        let ctx = result?;
        Ok(TlsConfig {
            ctx: Arc::new(ctx),
            endpoint_identification: ssl.endpoint_identification,
            enable_verification: ssl.enable_certificate_verification,
        })
    }

    /// Performs the TLS handshake over `tcp` for a broker with host name
    /// `host`, and checks the broker certificate like librdkafka's
    /// `rd_kafka_transport_ssl_verify`.
    pub(crate) async fn connect(
        &self,
        host: &str,
        tcp: TcpStream,
        logger: &Logger,
        broker_name: &str,
    ) -> Result<SslStream<TcpStream>, Failure> {
        let ssl = self
            .new_ssl(host)
            .map_err(|e| ssl_failure(e, logger, broker_name, RDKafkaErrorCode::SSL, ""))?;
        let mut stream = SslStream::new(ssl, tcp)
            .map_err(|e| ssl_failure(e, logger, broker_name, RDKafkaErrorCode::SSL, ""))?;
        if let Err(e) = Pin::new(&mut stream).connect().await {
            return Err(handshake_failure(&e, logger, broker_name));
        }
        if self.enable_verification {
            let ssl = stream.ssl();
            if ssl.peer_certificate().is_none() {
                return Err(Failure::error(
                    RDKafkaErrorCode::SSL,
                    "Broker did not provide a certificate",
                ));
            }
            let result = ssl.verify_result();
            if result != X509VerifyResult::OK {
                return Err(Failure::error(
                    RDKafkaErrorCode::SSL,
                    format!(
                        "Failed to verify broker certificate: {}",
                        result.error_string()
                    ),
                ));
            }
        }
        Ok(stream)
    }

    fn new_ssl(&self, host: &str) -> Result<Ssl, ErrorStack> {
        let mut ssl = Ssl::new(&self.ctx)?;
        // librdkafka sends SNI only for non-numeric host names.
        let is_ipv6 = host.contains(':')
            && host
                .chars()
                .all(|c| c.is_ascii_hexdigit() || ":.[]%".contains(c));
        let is_ipv4 = host.chars().all(|c| c.is_ascii_digit() || c == '.');
        if !is_ipv6 && !is_ipv4 {
            ssl.set_hostname(host)?;
        }
        if self.endpoint_identification {
            ssl.param_mut().set_host(host)?;
        }
        Ok(ssl)
    }
}

/// Formats one OpenSSL error like `ERR_error_string_n` plus librdkafka's
/// data suffix: `error:0A000086:SSL routines::certificate verify failed`.
///
/// With `debug` (the client's log level is debug), librdkafka prefixes the
/// OpenSSL source location: `ssl/statem/statem_clnt.c:2125:func error:...`.
fn format_openssl_error(e: &openssl::error::Error, debug: bool) -> String {
    let mut s = String::new();
    if debug {
        let _ = write!(
            s,
            "{}:{}:{} ",
            e.file(),
            e.line(),
            e.function().unwrap_or_default()
        );
    }
    let _ = write!(s, "error:{:08X}:", e.code());
    match e.library() {
        Some(lib) => s.push_str(lib),
        None => {
            let _ = write!(s, "lib({})", e.library_code());
        }
    }
    // OpenSSL 3 leaves the function name empty.
    s.push_str("::");
    match e.reason() {
        Some(reason) => s.push_str(reason),
        None => {
            let _ = write!(s, "reason({})", e.reason_code());
        }
    }
    if let Some(data) = e.data().filter(|d| !d.is_empty()) {
        s.push_str(": ");
        s.push_str(data);
    }
    s
}

/// librdkafka's `rd_kafka_ssl_error`: the last error of the stack; earlier
/// ones are logged with facility `SSL`. `prefix` is prepended to logged
/// lines (the broker name for connection errors).
fn openssl_error_text(stack: &ErrorStack, logger: &Logger, prefix: Option<&str>) -> String {
    let errors = stack.errors();
    let Some((last, earlier)) = errors.split_last() else {
        return "kafka: No further error information available".to_string();
    };
    let debug = logger.enabled(RDKafkaLogLevel::Debug);
    for e in earlier {
        let text = format_openssl_error(e, debug);
        match prefix {
            Some(p) => logger.log_fmt(
                RDKafkaLogLevel::Error,
                "SSL",
                format_args!("[thrd:{p}]: {p}: kafka: {text}"),
            ),
            None => logger.log_fmt(
                RDKafkaLogLevel::Error,
                "SSL",
                format_args!("[thrd:app]: kafka: {text}"),
            ),
        }
    }
    format_openssl_error(last, debug)
}

fn ssl_failure(
    stack: ErrorStack,
    logger: &Logger,
    broker_name: &str,
    code: RDKafkaErrorCode,
    prefix: &str,
) -> Failure {
    Failure::error(
        code,
        format!(
            "{prefix}{}",
            openssl_error_text(&stack, logger, Some(broker_name))
        ),
    )
}

/// The failure for a failed handshake, with librdkafka's hints.
fn handshake_failure(e: &openssl::ssl::Error, logger: &Logger, broker_name: &str) -> Failure {
    let errstr = ssl_error_text(e, logger, broker_name);
    let mut code = RDKafkaErrorCode::SSL;
    let extra = if errstr.contains("unexpected message") {
        ": client SSL authentication might be required (see ssl.key.location and \
         ssl.certificate.location and consult the broker logs for more information)"
    } else if errstr.contains("tls_process_server_certificate:certificate verify failed")
        || errstr.contains("error:0A000086")
        || errstr.contains("get_server_certificate:certificate verify failed")
    {
        ": broker certificate could not be verified, verify that ssl.ca.location is \
         correctly configured or root CA certificates are installed \
         (install ca-certificates package)"
    } else if errstr.starts_with("Disconnected") {
        code = RDKafkaErrorCode::BrokerTransportFailure;
        ": connecting to a PLAINTEXT broker listener?"
    } else {
        ""
    };
    Failure::error(code, format!("SSL handshake failed: {errstr}{extra}"))
}

/// The librdkafka text of an I/O error of a TLS stream, if it carries an
/// OpenSSL error.
pub(crate) fn io_error_text(e: &io::Error, logger: &Logger, broker_name: &str) -> Option<String> {
    let ssl = e.get_ref()?.downcast_ref::<openssl::ssl::Error>()?;
    Some(ssl_error_text(ssl, logger, broker_name))
}

/// librdkafka's `rd_kafka_transport_ssl_io_update` texts for a failed TLS
/// operation.
fn ssl_error_text(e: &openssl::ssl::Error, logger: &Logger, broker_name: &str) -> String {
    match e.ssl_error() {
        Some(stack) if !stack.errors().is_empty() => {
            openssl_error_text(stack, logger, Some(broker_name))
        }
        _ => match e.code() {
            openssl::ssl::ErrorCode::ZERO_RETURN => {
                "Disconnected: SSL connection closed by peer".to_string()
            }
            // With SSL_OP_IGNORE_UNEXPECTED_EOF, librdkafka sees a peer close
            // as SSL_ERROR_ZERO_RETURN; tokio-openssl's BIO reports it as a
            // syscall error without errno.
            openssl::ssl::ErrorCode::SYSCALL => match e.io_error() {
                None => "Disconnected: SSL connection closed by peer".to_string(),
                Some(io) if io.kind() == std::io::ErrorKind::UnexpectedEof => {
                    "Disconnected: SSL connection closed by peer".to_string()
                }
                Some(io) if io.kind() == std::io::ErrorKind::ConnectionReset => {
                    "Disconnected: connection reset by peer".to_string()
                }
                Some(io) => format!("SSL transport error: {}", super::connect::strerror(io)),
            },
            _ => match e.io_error() {
                Some(io) if io.kind() == std::io::ErrorKind::ConnectionReset => {
                    "Disconnected: connection reset by peer".to_string()
                }
                Some(io) => format!("SSL transport error: {}", super::connect::strerror(io)),
                None => "kafka: No further error information available".to_string(),
            },
        },
    }
}

/// A creation error: `context` followed by the OpenSSL error text.
fn creation_error(context: &str, stack: &ErrorStack, logger: &Logger) -> String {
    format!("{context}{}", openssl_error_text(stack, logger, None))
}

fn build_context(ssl: &SslSettings, logger: &Logger) -> Result<SslContext, String> {
    let mut b = SslContextBuilder::new(SslMethod::tls_client())
        .map_err(|e| creation_error("SSL_CTX_new() failed: ", &e, logger))?;
    b.set_options(SslOptions::NO_SSLV3);
    if let Some(ciphers) = &ssl.cipher_suites {
        b.set_cipher_list(ciphers)
            .map_err(|e| creation_error("ssl.cipher.suites failed: ", &e, logger))?;
    }
    b.set_verify(if ssl.enable_certificate_verification {
        SslVerifyMode::PEER
    } else {
        SslVerifyMode::NONE
    });
    if let Some(curves) = &ssl.curves_list {
        b.set_groups_list(curves)
            .map_err(|e| creation_error("ssl.curves.list failed: ", &e, logger))?;
    }
    if let Some(sigalgs) = &ssl.sigalgs_list {
        b.set_sigalgs_list(sigalgs)
            .map_err(|e| creation_error("ssl.sigalgs.list failed: ", &e, logger))?;
    }
    set_certs(&mut b, ssl, logger)?;
    b.set_options(SslOptions::IGNORE_UNEXPECTED_EOF);
    Ok(b.build())
}

/// The password callback librdkafka installs: `ssl.key.password`, or empty.
fn password_cb(password: Option<&str>) -> impl FnOnce(&mut [u8]) -> Result<usize, ErrorStack> + '_ {
    move |buf: &mut [u8]| {
        let pw = password.unwrap_or("").as_bytes();
        let n = pw.len().min(buf.len());
        buf[..n].copy_from_slice(&pw[..n]);
        Ok(n)
    }
}

fn read_private_key(pem: &[u8], password: Option<&str>) -> Result<PKey<Private>, ErrorStack> {
    PKey::private_key_from_pem_callback(pem, password_cb(password))
}

/// The PEM block types `PEM_read_bio_X509` accepts.
const CERT_PEM_TYPES: &[&str] = &["CERTIFICATE", "X509 CERTIFICATE", "TRUSTED CERTIFICATE"];

/// Splits PEM text into the certificate blocks `PEM_read_bio_X509` would
/// read in a loop, skipping other block types and surrounding text.
fn cert_pem_blocks(pem: &str) -> Vec<&str> {
    let mut blocks = Vec::new();
    let mut rest = pem;
    while let Some(start) = rest.find("-----BEGIN ") {
        let after = &rest[start + "-----BEGIN ".len()..];
        let Some(name_end) = after.find("-----") else {
            break;
        };
        let name = &after[..name_end];
        let end_marker = format!("-----END {name}-----");
        let Some(end) = after.find(&end_marker) else {
            // An unterminated block: let OpenSSL report it.
            blocks.push(&rest[start..]);
            break;
        };
        let block_end = start + "-----BEGIN ".len() + end + end_marker.len();
        if CERT_PEM_TYPES.contains(&name) {
            blocks.push(&rest[start..block_end]);
        }
        rest = &rest[block_end..];
    }
    blocks
}

/// Reports whether `block`, a subslice of `pem`, is followed only by
/// whitespace.
fn is_last_block(pem: &str, block: &str) -> bool {
    let start = block.as_ptr().addr() - pem.as_ptr().addr();
    pem[start + block.len()..].trim().is_empty()
}

fn set_certs(b: &mut SslContextBuilder, ssl: &SslSettings, logger: &Logger) -> Result<(), String> {
    let mut ca_probe = true;
    if let Some(location) = ssl.ca_location.as_deref().filter(|l| *l != "probe") {
        let path = Path::new(location);
        let result = if path.is_dir() {
            b.load_verify_locations(None, Some(path))
        } else {
            b.load_verify_locations(Some(path), None)
        };
        result.map_err(|e| creation_error("ssl.ca.location failed: ", &e, logger))?;
        ca_probe = false;
    }
    if let Some(pem) = &ssl.ca_pem {
        let blocks = cert_pem_blocks(pem);
        if blocks.is_empty() {
            // Let OpenSSL produce its "no start line" error.
            let err = X509::from_pem(pem.as_bytes())
                .err()
                .unwrap_or_else(ErrorStack::get);
            return Err(creation_error(
                "failed to read certificate #0 from ssl.ca.pem: not in PEM format?: ",
                &err,
                logger,
            ));
        }
        for (i, block) in blocks.iter().enumerate() {
            let cert = match X509::from_pem(block.as_bytes()) {
                Ok(cert) => cert,
                // librdkafka reads certificates until one fails, then
                // requires the input to be exhausted: a malformed last block
                // after at least one good one ends the list without error.
                Err(_) if i > 0 && is_last_block(pem, block) => break,
                Err(e) => {
                    return Err(creation_error(
                        &format!(
                            "failed to read certificate #{i} from ssl.ca.pem: not in PEM format?: "
                        ),
                        &e,
                        logger,
                    ));
                }
            };
            b.cert_store_mut().add_cert(cert).map_err(|e| {
                creation_error(
                    &format!("failed to add ssl.ca.pem certificate #{i} to CA cert store: "),
                    &e,
                    logger,
                )
            })?;
        }
        ca_probe = false;
    }
    if ca_probe {
        let probed = CA_PROBE_PATHS.iter().any(|p| {
            let path = Path::new(p);
            let result = if path.is_dir() {
                b.load_verify_locations(None, Some(path))
            } else if path.is_file() {
                b.load_verify_locations(Some(path), None)
            } else {
                return false;
            };
            result.is_ok()
        });
        if !probed {
            // Ignore failures, as librdkafka does.
            let _ = b.set_default_verify_paths();
        }
    }

    let mut check_pkey = false;
    if let Some(location) = &ssl.certificate_location {
        b.set_certificate_chain_file(location)
            .map_err(|e| creation_error("ssl.certificate.location failed: ", &e, logger))?;
    }
    if let Some(pem) = &ssl.certificate_pem {
        let mut certs = X509::stack_from_pem(pem.as_bytes())
            .ok()
            .filter(|c| !c.is_empty())
            .ok_or_else(|| {
                let err = X509::from_pem(pem.as_bytes())
                    .err()
                    .unwrap_or_else(ErrorStack::get);
                creation_error(
                    "ssl.certificate.pem failed: not in PEM format?: ",
                    &err,
                    logger,
                )
            })?
            .into_iter();
        let main = certs.next().expect("non-empty");
        b.set_certificate(&main).map_err(|e| {
            creation_error(
                "ssl.certificate.pem failed: setting main certificate: ",
                &e,
                logger,
            )
        })?;
        for cert in certs {
            b.add_extra_chain_cert(cert).map_err(|e| {
                creation_error(
                    "ssl.certificate.pem failed: setting certificate chain: ",
                    &e,
                    logger,
                )
            })?;
        }
    }
    let password = ssl.key_password.as_deref();
    if let Some(location) = &ssl.key_location {
        match password {
            // Let OpenSSL read the file, for its error texts.
            None => b
                .set_private_key_file(location, SslFiletype::PEM)
                .map_err(|e| creation_error("ssl.key.location failed: ", &e, logger))?,
            // The crate offers no context password callback: decrypt here.
            Some(_) => {
                let key = std::fs::read(location)
                    .map_err(|e| format!("ssl.key.location failed: {e}"))
                    .and_then(|pem| {
                        read_private_key(&pem, password)
                            .map_err(|e| creation_error("ssl.key.location failed: ", &e, logger))
                    })?;
                b.set_private_key(&key)
                    .map_err(|e| creation_error("ssl.key.location failed: ", &e, logger))?;
            }
        }
        check_pkey = true;
    }
    if let Some(pem) = &ssl.key_pem {
        let key = read_private_key(pem.as_bytes(), password)
            .map_err(|e| creation_error("ssl.key.pem failed: not in PEM format?: ", &e, logger))?;
        b.set_private_key(&key)
            .map_err(|e| creation_error("ssl.key.pem failed: ", &e, logger))?;
        check_pkey = true;
    }
    if let Some(location) = &ssl.keystore_location {
        let der = std::fs::read(location)
            .map_err(|e| format!("Failed to open ssl.keystore.location: {location}: {e}"))?;
        let p12 = Pkcs12::from_der(&der).map_err(|e| {
            creation_error(
                &format!("Error reading ssl.keystore.location PKCS#12 file: {location}: "),
                &e,
                logger,
            )
        })?;
        let parsed = p12
            .parse2(ssl.keystore_password.as_deref().unwrap_or(""))
            .map_err(|e| {
                creation_error(
                    &format!("Failed to parse ssl.keystore.location PKCS#12 file: {location}: "),
                    &e,
                    logger,
                )
            })?;
        let use_err =
            |e: ErrorStack| creation_error("Failed to use ssl.keystore.location: ", &e, logger);
        if let Some(cert) = &parsed.cert {
            b.set_certificate(cert).map_err(use_err)?;
        }
        if let Some(key) = &parsed.pkey {
            b.set_private_key(key).map_err(use_err)?;
        }
        if let Some(ca) = parsed.ca {
            for cert in ca {
                b.add_extra_chain_cert(cert).map_err(use_err)?;
            }
        }
        check_pkey = true;
    }
    if check_pkey {
        b.check_private_key()
            .map_err(|e| creation_error("Private key check failed: ", &e, logger))?;
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn pem_blocks() {
        let pem = "junk\n-----BEGIN CERTIFICATE-----\nAAA\n-----END CERTIFICATE-----\n\
                   -----BEGIN PRIVATE KEY-----\nBBB\n-----END PRIVATE KEY-----\n\
                   -----BEGIN TRUSTED CERTIFICATE-----\nCCC\n-----END TRUSTED CERTIFICATE-----\ntrailer";
        let blocks = cert_pem_blocks(pem);
        assert_eq!(blocks.len(), 2);
        assert!(blocks[0].contains("AAA"));
        assert!(blocks[1].contains("CCC"));
        assert!(cert_pem_blocks("no pem here").is_empty());
        let unterminated = cert_pem_blocks("-----BEGIN CERTIFICATE-----\nAAA");
        assert_eq!(unterminated.len(), 1);
    }

    /// A self-signed certificate and its key, PEM encoded.
    pub(crate) fn self_signed(cn: &str) -> (String, String) {
        use openssl::asn1::Asn1Time;
        use openssl::bn::BigNum;
        use openssl::ec::{EcGroup, EcKey};
        use openssl::hash::MessageDigest;
        use openssl::nid::Nid;
        use openssl::x509::X509NameBuilder;

        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
        let key = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", cn).unwrap();
        let name = name.build();
        let mut b = X509::builder().unwrap();
        b.set_version(2).unwrap();
        b.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap())
            .unwrap();
        b.set_subject_name(&name).unwrap();
        b.set_issuer_name(&name).unwrap();
        b.set_pubkey(&key).unwrap();
        b.set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        b.set_not_after(&Asn1Time::days_from_now(1).unwrap())
            .unwrap();
        b.sign(&key, MessageDigest::sha256()).unwrap();
        let cert = String::from_utf8(b.build().to_pem().unwrap()).unwrap();
        let key = String::from_utf8(key.private_key_to_pem_pkcs8().unwrap()).unwrap();
        (cert, key)
    }

    fn create(pairs: &[(&str, &str)]) -> Result<(), String> {
        use crate::client::DefaultClientContext;
        use crate::config::ClientConfig;
        use crate::config::settings::{ClientKind, ClientSettings};
        use crate::internal::hooks::Hooks;

        let mut config = ClientConfig::new();
        config.set("security.protocol", "ssl");
        for (k, v) in pairs {
            config.set(*k, *v);
        }
        let settings = ClientSettings::from_config(&config, ClientKind::Consumer).unwrap();
        let hooks: Arc<dyn Hooks> = Arc::new(DefaultClientContext);
        let logger = Logger::for_creation(hooks, &settings);
        let result = TlsConfig::new(&settings.ssl, &logger).map(|_| ());
        assert!(
            ErrorStack::get().errors().is_empty(),
            "OpenSSL error queue not empty"
        );
        result
    }

    /// The client creation texts of librdkafka 2.12.1 (recorded with the
    /// differential harness), which Materialize's tests match on.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // OpenSSL
    fn creation_errors_match_librdkafka() {
        let (cert, key) = self_signed("kafka");
        let (_, other_key) = self_signed("other");
        assert_eq!(
            create(&[("ssl.ca.pem", "garbage")]),
            Err(
                "failed to read certificate #0 from ssl.ca.pem: not in PEM format?: \
                 error:0480006C:PEM routines::no start line: Expecting: CERTIFICATE"
                    .into()
            )
        );
        // A malformed last certificate after a good one is ignored, as in
        // librdkafka.
        let ca = format!("{cert}-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n");
        assert_eq!(create(&[("ssl.ca.pem", &ca)]), Ok(()));
        let two = format!("{cert}{cert}");
        assert_eq!(create(&[("ssl.ca.pem", &two)]), Ok(()));
        // ... but not a malformed one followed by more.
        let ca =
            format!("{cert}-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n{cert}");
        assert!(
            create(&[("ssl.ca.pem", &ca)])
                .unwrap_err()
                .starts_with("failed to read certificate #1 from ssl.ca.pem: not in PEM format?: ")
        );
        assert_eq!(
            create(&[("ssl.certificate.pem", &cert), ("ssl.key.pem", "garbage")]),
            Err(
                "ssl.key.pem failed: not in PEM format?: error:1E08010C:DECODER \
                 routines::unsupported: No supported data to decode. Input type: PEM"
                    .into()
            )
        );
        assert_eq!(
            create(&[("ssl.certificate.pem", "garbage"), ("ssl.key.pem", &key)]),
            Err(
                "ssl.certificate.pem failed: not in PEM format?: error:0480006C:PEM \
                 routines::no start line: Expecting: CERTIFICATE"
                    .into()
            )
        );
        assert_eq!(
            create(&[("ssl.certificate.pem", &cert), ("ssl.key.pem", &other_key)]),
            Err(
                "ssl.key.pem failed: error:05800074:x509 certificate routines::key values mismatch"
                    .into()
            )
        );
        assert_eq!(
            create(&[("ssl.key.pem", &key)]),
            Err(
                "Private key check failed: error:0A0000B1:SSL routines::no certificate assigned"
                    .into()
            )
        );
        assert_eq!(
            create(&[("ssl.certificate.pem", &cert), ("ssl.key.pem", &key)]),
            Ok(())
        );
        assert_eq!(create(&[]), Ok(()));
        let err = create(&[("ssl.ca.location", "/nonexistent/ca.crt")]).unwrap_err();
        assert_eq!(
            err,
            "ssl.ca.location failed: error:05880020:x509 certificate routines::BIO lib"
        );
        let err = create(&[
            ("ssl.certificate.pem", &cert),
            ("ssl.key.location", "/nonexistent/key"),
        ])
        .unwrap_err();
        assert_eq!(
            err,
            "ssl.key.location failed: error:0A080002:SSL routines::system lib"
        );
    }
}
