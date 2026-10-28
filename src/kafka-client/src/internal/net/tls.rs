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
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use openssl::error::ErrorStack;
use openssl::pkcs12::Pkcs12;
use openssl::pkey::{PKey, Private};
use openssl::ssl::{Ssl, SslContext, SslContextBuilder, SslMethod, SslOptions, SslVerifyMode};
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
fn format_openssl_error(e: &openssl::error::Error) -> String {
    let mut s = format!("error:{:08X}:", e.code());
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
    for e in earlier {
        let text = format_openssl_error(e);
        match prefix {
            Some(p) => logger.log_fmt(
                RDKafkaLogLevel::Error,
                "SSL",
                format_args!("{p}: kafka: {text}"),
            ),
            None => logger.log_fmt(RDKafkaLogLevel::Error, "SSL", format_args!("kafka: {text}")),
        }
    }
    format_openssl_error(last)
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
    let errstr = match e.ssl_error() {
        Some(stack) if !stack.errors().is_empty() => {
            openssl_error_text(stack, logger, Some(broker_name))
        }
        _ => match e.code() {
            openssl::ssl::ErrorCode::ZERO_RETURN => {
                "Disconnected: SSL connection closed by peer".to_string()
            }
            openssl::ssl::ErrorCode::SYSCALL => match e.io_error() {
                None => "Disconnected: connection closed by peer".to_string(),
                Some(io) if io.kind() == std::io::ErrorKind::UnexpectedEof => {
                    "Disconnected: connection closed by peer".to_string()
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
    };
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
            let cert = X509::from_pem(block.as_bytes()).map_err(|e| {
                creation_error(
                    &format!(
                        "failed to read certificate #{i} from ssl.ca.pem: not in PEM format?: "
                    ),
                    &e,
                    logger,
                )
            })?;
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
        let key = std::fs::read(location)
            .map_err(|e| format!("ssl.key.location failed: {e}"))
            .and_then(|pem| {
                read_private_key(&pem, password)
                    .map_err(|e| creation_error("ssl.key.location failed: ", &e, logger))
            })?;
        b.set_private_key(&key)
            .map_err(|e| creation_error("ssl.key.location failed: ", &e, logger))?;
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
}
