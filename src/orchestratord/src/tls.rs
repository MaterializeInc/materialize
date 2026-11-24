// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use anyhow::bail;
use serde::Deserialize;

use mz_cloud_resources::crd::{
    ManagedResource, MaterializeCertSpec, MaterializeExternalCertSpec,
    generated::cert_manager::certificates::{
        Certificate, CertificatePrivateKey, CertificatePrivateKeyAlgorithm,
        CertificatePrivateKeyEncoding, CertificatePrivateKeyRotationPolicy, CertificateSpec,
    },
};

#[derive(Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DefaultCertificateSpecs {
    pub balancerd_external: Option<MaterializeCertSpec>,
    pub console_external: Option<MaterializeCertSpec>,
    pub internal: Option<MaterializeCertSpec>,
}

impl FromStr for DefaultCertificateSpecs {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}

pub fn create_certificate(
    default_spec: Option<MaterializeCertSpec>,
    resource: &impl ManagedResource,
    mz_cert_spec: Option<MaterializeCertSpec>,
    cert_name: String,
    secret_name: String,
    additional_dns_names: Option<Vec<String>>,
    algorithm_hint: CertificatePrivateKeyAlgorithm,
    size_hint: Option<i64>,
) -> anyhow::Result<Option<Certificate>> {
    let default_spec = default_spec.unwrap_or_else(MaterializeCertSpec::default);
    let mz_cert_spec = mz_cert_spec.unwrap_or_else(MaterializeCertSpec::default);
    let Some(issuer_ref) = mz_cert_spec.issuer_ref.or(default_spec.issuer_ref) else {
        return Ok(None);
    };
    let mut secret_template = mz_cert_spec
        .secret_template
        .or(default_spec.secret_template)
        .unwrap_or_default();
    secret_template.labels = Some(
        secret_template
            .labels
            .unwrap_or_default()
            .into_iter()
            .chain(resource.default_labels())
            .collect(),
    );
    let mut dns_names = mz_cert_spec
        .dns_names
        .or(default_spec.dns_names)
        .unwrap_or_default();
    if let Some(names) = additional_dns_names {
        dns_names.extend(names);
    }
    let private_key_algorithm = mz_cert_spec
        .private_key_algorithm
        .or(default_spec.private_key_algorithm)
        .unwrap_or(algorithm_hint);
    let private_key_size = match private_key_algorithm {
        CertificatePrivateKeyAlgorithm::Rsa => {
            let size = mz_cert_spec
                .private_key_size
                .or(default_spec.private_key_size)
                .unwrap_or_else(|| size_hint.unwrap_or(4096));
            if size < 2048 {
                bail!("RSA key size must be at least 2048 bits");
            }
            Some(size)
        }
        CertificatePrivateKeyAlgorithm::Ecdsa => {
            let size = mz_cert_spec
                .private_key_size
                .or(default_spec.private_key_size)
                .unwrap_or_else(|| size_hint.unwrap_or(256));
            if ![256, 384, 521].contains(&size) {
                bail!("ECDSA key size must be one of 256, 384, or 521 bits");
            }
            Some(size)
        }
        CertificatePrivateKeyAlgorithm::Ed25519 => None,
    };
    Ok(Some(Certificate {
        metadata: resource.managed_resource_meta(cert_name),
        spec: CertificateSpec {
            dns_names: Some(dns_names),
            duration: mz_cert_spec.duration.or(default_spec.duration),
            issuer_ref,
            private_key: Some(CertificatePrivateKey {
                algorithm: Some(private_key_algorithm),
                encoding: Some(CertificatePrivateKeyEncoding::Pkcs8),
                rotation_policy: Some(CertificatePrivateKeyRotationPolicy::Always),
                size: private_key_size,
            }),
            renew_before: mz_cert_spec.renew_before.or(default_spec.renew_before),
            secret_name,
            secret_template: Some(secret_template),
            ..Default::default()
        },
        status: None,
    }))
}

/// Checks that an external certificate spec does not ask for both a
/// cert-manager Certificate and an existing Secret.
pub fn validate_external_certificate_spec(
    spec: &Option<MaterializeExternalCertSpec>,
) -> anyhow::Result<()> {
    let Some(spec) = spec else {
        return Ok(());
    };
    match &spec.secret_name {
        Some(name) if name.is_empty() => bail!("secretName must not be empty"),
        Some(_) if spec.cert_manager.issuer_ref.is_some() => {
            bail!("secretName and issuerRef cannot both be set")
        }
        _ => Ok(()),
    }
}

/// The existing Secret named in an external certificate spec, if any.
pub fn existing_certificate_secret(spec: &Option<MaterializeExternalCertSpec>) -> Option<&str> {
    spec.as_ref()?.secret_name.as_deref()
}

/// Builds the cert-manager Certificate for an external certificate spec.
///
/// Returns `None` when the spec names an existing Secret, or when no issuer is
/// configured.
pub fn create_external_certificate(
    default_spec: Option<MaterializeCertSpec>,
    resource: &impl ManagedResource,
    spec: Option<MaterializeExternalCertSpec>,
    cert_name: String,
    secret_name: String,
    algorithm_hint: CertificatePrivateKeyAlgorithm,
    size_hint: Option<i64>,
) -> anyhow::Result<Option<Certificate>> {
    validate_external_certificate_spec(&spec)?;
    if existing_certificate_secret(&spec).is_some() {
        return Ok(None);
    }
    create_certificate(
        default_spec,
        resource,
        spec.map(|spec| spec.cert_manager),
        cert_name,
        secret_name,
        None,
        algorithm_hint,
        size_hint,
    )
}

/// The name of the Secret that holds an external certificate, or `None` when
/// the component should not serve TLS.
///
/// `managed_secret_name` is the Secret that cert-manager writes the
/// operator-managed Certificate to.
pub fn external_certificate_secret_name(
    defaults: &Option<MaterializeCertSpec>,
    spec: &Option<MaterializeExternalCertSpec>,
    managed_secret_name: String,
) -> Option<String> {
    if let Some(name) = existing_certificate_secret(spec) {
        return Some(name.to_owned());
    }
    let cert_manager = spec.as_ref().map(|spec| &spec.cert_manager);
    let issuer_ref_defined = cert_manager
        .and_then(|spec| spec.issuer_ref.as_ref())
        .or_else(|| defaults.as_ref().and_then(|spec| spec.issuer_ref.as_ref()))
        .is_some();
    issuer_ref_defined.then_some(managed_secret_name)
}

pub fn issuer_ref_defined(
    defaults: &Option<MaterializeCertSpec>,
    overrides: &Option<MaterializeCertSpec>,
) -> bool {
    overrides
        .as_ref()
        .and_then(|spec| spec.issuer_ref.as_ref())
        .is_some()
        || defaults
            .as_ref()
            .and_then(|spec| spec.issuer_ref.as_ref())
            .is_some()
}

pub fn resolved_dns_names(
    defaults: &Option<MaterializeCertSpec>,
    overrides: &Option<MaterializeExternalCertSpec>,
) -> Option<Vec<String>> {
    overrides
        .as_ref()
        .and_then(|spec| spec.cert_manager.dns_names.clone())
        .or_else(|| defaults.as_ref().and_then(|spec| spec.dns_names.clone()))
}

#[cfg(test)]
mod tests {
    use mz_cloud_resources::crd::generated::cert_manager::certificates::CertificateIssuerRef;

    use super::*;

    fn issuer_spec() -> MaterializeCertSpec {
        MaterializeCertSpec {
            issuer_ref: Some(CertificateIssuerRef {
                name: "issuer".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn secret_spec(name: &str) -> MaterializeExternalCertSpec {
        MaterializeExternalCertSpec {
            secret_name: Some(name.to_owned()),
            ..Default::default()
        }
    }

    #[mz_ore::test]
    fn external_certificate_secret_name_precedence() {
        let managed = || "managed-tls".to_owned();
        let secret_name = external_certificate_secret_name;
        // Neither an issuer nor a Secret: no TLS.
        assert_eq!(secret_name(&None, &None, managed()), None);
        // An issuer, from the defaults or the resource: the managed Secret.
        assert_eq!(
            secret_name(&Some(issuer_spec()), &None, managed()),
            Some(managed())
        );
        assert_eq!(
            secret_name(&None, &Some(issuer_spec().into()), managed()),
            Some(managed())
        );
        // An existing Secret wins over the default issuer.
        assert_eq!(
            secret_name(&Some(issuer_spec()), &Some(secret_spec("tls")), managed()),
            Some("tls".to_owned())
        );
    }

    #[mz_ore::test]
    fn validate_external_certificate_spec_rejects_conflicts() {
        assert!(validate_external_certificate_spec(&None).is_ok());
        assert!(validate_external_certificate_spec(&Some(issuer_spec().into())).is_ok());
        assert!(validate_external_certificate_spec(&Some(secret_spec("tls"))).is_ok());
        assert!(validate_external_certificate_spec(&Some(secret_spec(""))).is_err());
        let both = MaterializeExternalCertSpec {
            cert_manager: issuer_spec(),
            secret_name: Some("tls".to_owned()),
        };
        assert!(validate_external_certificate_spec(&Some(both)).is_err());
    }
}
