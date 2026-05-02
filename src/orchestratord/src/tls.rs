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
    ManagedResource, MaterializeCertSpec,
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
