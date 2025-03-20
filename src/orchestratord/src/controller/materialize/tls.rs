// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_cloud_resources::crd::gen::cert_manager::certificates::{
    Certificate, CertificatePrivateKey, CertificatePrivateKeyAlgorithm,
    CertificatePrivateKeyEncoding, CertificatePrivateKeyRotationPolicy, CertificateSpec,
};
use mz_cloud_resources::crd::materialize::v1alpha1::{Materialize, MaterializeCertSpec};

pub fn create_certificate(
    default_spec: Option<MaterializeCertSpec>,
    mz: &Materialize,
    mz_cert_spec: Option<MaterializeCertSpec>,
    cert_name: String,
    secret_name: String,
    additional_dns_names: Option<Vec<String>>,
) -> Option<Certificate> {
    let default_spec = default_spec.unwrap_or_else(MaterializeCertSpec::default);
    let mz_cert_spec = mz_cert_spec.unwrap_or_else(MaterializeCertSpec::default);
    let Some(issuer_ref) = mz_cert_spec.issuer_ref.or(default_spec.issuer_ref) else {
        return None;
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
            .chain(mz.default_labels())
            .collect(),
    );
    let mut dns_names = mz_cert_spec
        .dns_names
        .or(default_spec.dns_names)
        .unwrap_or_default();
    if let Some(names) = additional_dns_names {
        dns_names.extend(names);
    }
    Some(Certificate {
        metadata: mz.managed_resource_meta(cert_name),
        spec: CertificateSpec {
            dns_names: Some(dns_names),
            duration: mz_cert_spec.duration.or(default_spec.duration),
            issuer_ref,
            private_key: Some(CertificatePrivateKey {
                algorithm: Some(CertificatePrivateKeyAlgorithm::Rsa),
                encoding: Some(CertificatePrivateKeyEncoding::Pkcs8),
                rotation_policy: Some(CertificatePrivateKeyRotationPolicy::Always),
                size: Some(4096),
            }),
            renew_before: mz_cert_spec.renew_before.or(default_spec.renew_before),
            secret_name,
            secret_template: Some(secret_template),
            ..Default::default()
        },
        status: None,
    })
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
