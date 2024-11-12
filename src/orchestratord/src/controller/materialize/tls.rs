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
    mz: &Materialize,
    mz_cert_spec: &MaterializeCertSpec,
    cert_name: String,
    secret_name: String,
    additional_dns_names: Option<Vec<String>>,
) -> Certificate {
    let mut secret_template = mz_cert_spec.secret_template.clone().unwrap_or_default();
    secret_template.labels = Some(
        secret_template
            .labels
            .unwrap_or_default()
            .into_iter()
            .chain(mz.default_labels())
            .collect(),
    );
    let mut dns_names = Vec::new();
    if let Some(names) = mz_cert_spec.dns_names.clone() {
        dns_names.extend(names);
    }
    if let Some(names) = additional_dns_names {
        dns_names.extend(names);
    }
    Certificate {
        metadata: mz.managed_resource_meta(cert_name),
        spec: CertificateSpec {
            dns_names: Some(dns_names),
            duration: mz_cert_spec.duration.clone(),
            issuer_ref: mz_cert_spec.issuer_ref.clone(),
            private_key: Some(CertificatePrivateKey {
                algorithm: Some(CertificatePrivateKeyAlgorithm::Ed25519),
                encoding: Some(CertificatePrivateKeyEncoding::Pkcs8),
                rotation_policy: Some(CertificatePrivateKeyRotationPolicy::Always),
                size: None,
            }),
            renew_before: mz_cert_spec.renew_before.clone(),
            secret_name,
            secret_template: Some(secret_template),
            ..Default::default()
        },
        status: None,
    }
}
