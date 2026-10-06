// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Labels and annotations that let the `teleport-kube-agent` already
//! deployed in every cluster discover the environmentd Service and
//! register it as a Teleport `app`, via Kubernetes annotation-based
//! auto-discovery.
//!
//! This is Phase 1 of moving `environmentd` Teleport registration off of
//! `environment-controller`'s `tctl`-based flow. It registers the `app`
//! only; the `db` stays on `environment-controller` until Phase 2 installs
//! the Teleport Kubernetes Operator.

use std::collections::BTreeMap;

use data_encoding::BASE32_NOPAD;
use k8s_openapi::api::core::v1::Service;
use kube::ResourceExt;
use mz_cloud_provider::CloudProvider;
use mz_cloud_resources::crd::materialize::v1alpha1::Materialize;
use serde::Serialize;
use tracing::error;
use uuid::Uuid;

use super::Config;

/// The named environmentd Service port to register.
///
/// NOTE: this is a security control, not tidiness. Without it the agent reads
/// every port on the Service, and its protocol detection matches the port name
/// before it pings anything, so the port named `https` registers as an app.
/// That is the customer-facing endpoint, and every app the agent builds from
/// one Service inherits that Service's `teleport.dev/app-rewrite`, so it would
/// arrive carrying the `mz_support` header.
const TELEPORT_APP_PORT: &str = "internal-http";

/// The internal HTTP endpoint is plain HTTP, not HTTPS. Set explicitly rather
/// than relying on Teleport's port-name heuristic to guess it.
///
/// NOTE: the agent applies this to every port it selects, so it is only safe
/// alongside `TELEPORT_APP_PORT`. On its own it would force `http` on all four
/// ports and register four apps.
const TELEPORT_APP_PROTOCOL: &str = "http";

const TELEPORT_APP_DESCRIPTION: &str = "Environmentd Internal HTTP API";

/// Forces the support database user on internal HTTP requests reaching the
/// registered app. This is a security control, not a convenience: it is
/// what stops a support session from picking a different database role.
const TELEPORT_REWRITE_HEADER_NAME: &str = "X-Materialize-User";
const TELEPORT_REWRITE_HEADER_VALUE: &str = "mz_support";

/// The settings that the `--teleport-*` flags provide.
#[derive(Clone)]
pub struct TeleportConfig {
    pub endpoint: String,
    pub stack_type: String,
    pub cluster_name: String,
}

#[derive(Serialize)]
struct RewriteHeader {
    name: String,
    value: String,
}

#[derive(Serialize)]
struct Rewrite {
    headers: Vec<RewriteHeader>,
}

/// The trailing ordinal of a Materialize resource name, named
/// `environment-{org_uuid}-{ordinal}`.
///
/// `environment-controller` derives the ordinal the same way, in `ordinal()`
/// in the `cloud` repo (`src/environment/src/util.rs`). The two must agree,
/// because the un-prefixed form of the name this module builds has to equal
/// the one `environment-controller` registers.
fn ordinal(resource_name: &str) -> &str {
    resource_name.split('-').next_back().unwrap_or("0")
}

/// The Teleport `app` resource name: `mz-{cloud_provider}-{region}-{base32(org_uuid)}-{ordinal}`.
///
/// The `mz-` prefix lets this registration coexist with the `app`
/// `environment-controller` registers under the un-prefixed name. Add the
/// prefix only here, never in `Materialize::environment_id`, which other
/// consumers (the `--environment-id` flag, KMS key aliases) rely on staying
/// stable.
///
/// Returns an error when the result is not a valid DNS-1035 label, which
/// Teleport requires. A long enough cloud provider and region can exceed the
/// 63-character limit: `generic` plus a 23-character region leaves room for a
/// single-digit ordinal and no more.
fn teleport_app_name(
    cloud_provider: CloudProvider,
    region: &str,
    environment_id: Uuid,
    ordinal: &str,
) -> Result<String, String> {
    let name = format!(
        "mz-{}-{}-{}-{}",
        cloud_provider,
        region,
        BASE32_NOPAD
            .encode(environment_id.as_bytes())
            .to_lowercase(),
        ordinal,
    );
    match dns1035_label_error(&name) {
        Some(reason) => Err(format!("{name:?} is not a valid DNS-1035 label: {reason}")),
        None => Ok(name),
    }
}

/// Why `name` is not a valid DNS-1035 label, or `None` when it is one.
///
/// Mirrors `validation.IsDNS1035Label`, which Teleport runs on the
/// `teleport.dev/name` annotation. A name that fails it registers nothing at
/// all: the agent logs one warning and drops the whole Service, so the
/// environment never appears in Teleport.
fn dns1035_label_error(name: &str) -> Option<&'static str> {
    if name.len() > 63 {
        return Some("longer than 63 characters");
    }
    if !name.starts_with(|c: char| c.is_ascii_lowercase()) {
        return Some("does not start with a lowercase letter");
    }
    if !name.ends_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit()) {
        return Some("does not end with a lowercase letter or digit");
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Some("contains a character outside [-a-z0-9]");
    }
    None
}

/// Adds the Teleport discovery labels and annotations to `service` when
/// `--teleport-endpoint` is set. A no-op otherwise, which is the rollback
/// path: absent the flag, this function changes nothing about the Service.
///
/// A Service the agent would reject is left unannotated rather than annotated
/// and silently dropped, so the reason reaches our own logs instead of only
/// the agent's.
pub(super) fn apply_teleport_registration(
    config: &Config,
    mz: &Materialize,
    service: &mut Service,
) {
    let Some(teleport) = &config.teleport else {
        return;
    };

    let resource_name = mz.name_unchecked();
    let app_name = match teleport_app_name(
        config.cloud_provider,
        &config.region,
        mz.spec.environment_id,
        ordinal(&resource_name),
    ) {
        Ok(app_name) => app_name,
        Err(reason) => {
            error!(%resource_name, "skipping Teleport registration: {reason}");
            return;
        }
    };

    let labels = service.metadata.labels.get_or_insert_with(BTreeMap::new);
    labels.insert(
        "materialize.cloud/app".to_string(),
        mz.environmentd_app_name(),
    );
    labels.insert("stack-type".to_string(), teleport.stack_type.clone());
    labels.insert("cluster".to_string(), teleport.cluster_name.clone());

    let rewrite = Rewrite {
        headers: vec![RewriteHeader {
            name: TELEPORT_REWRITE_HEADER_NAME.to_string(),
            value: TELEPORT_REWRITE_HEADER_VALUE.to_string(),
        }],
    };
    let annotations = service
        .metadata
        .annotations
        .get_or_insert_with(BTreeMap::new);
    annotations.insert("teleport.dev/name".to_string(), app_name);
    annotations.insert(
        "teleport.dev/port".to_string(),
        TELEPORT_APP_PORT.to_string(),
    );
    annotations.insert(
        "teleport.dev/protocol".to_string(),
        TELEPORT_APP_PROTOCOL.to_string(),
    );
    annotations.insert(
        "teleport.dev/description".to_string(),
        TELEPORT_APP_DESCRIPTION.to_string(),
    );
    annotations.insert(
        "teleport.dev/app-rewrite".to_string(),
        serde_yaml::to_string(&rewrite).expect("Rewrite has no non-serializable fields"),
    );
}

#[cfg(test)]
mod tests {
    use uuid::uuid;

    use super::*;

    const ORG: Uuid = uuid!("01d730c7-5a61-4d7a-9f3c-63ed01f34ebf");

    // The expected value is `environment_id_base32_from_env`'s own fixture in
    // the `cloud` repo (`src/environment/src/util.rs`), with the prefix added.
    #[mz_ore::test]
    fn app_name_matches_the_base32_convention() {
        assert_eq!(
            teleport_app_name(
                CloudProvider::Local,
                "kind",
                ORG,
                ordinal("environment-01d730c7-5a61-4d7a-9f3c-63ed01f34ebf-0"),
            ),
            Ok("mz-local-kind-ahltbr22mfgxvhz4mpwqd42ox4-0".to_string()),
        );
    }

    #[mz_ore::test]
    fn app_name_carries_a_non_zero_ordinal() {
        assert_eq!(
            teleport_app_name(
                CloudProvider::Local,
                "kind",
                ORG,
                ordinal("environment-01d730c7-5a61-4d7a-9f3c-63ed01f34ebf-12"),
            ),
            Ok("mz-local-kind-ahltbr22mfgxvhz4mpwqd42ox4-12".to_string()),
        );
    }

    // The longest region in `infra/` is 14 characters, so every region we
    // deploy today clears the limit with room to spare.
    #[mz_ore::test]
    fn app_name_fits_the_longest_region_we_deploy() {
        let name = teleport_app_name(CloudProvider::Aws, "ap-southeast-2", ORG, "0")
            .expect("valid DNS-1035 label");
        assert_eq!(name.len(), 50);
    }

    #[mz_ore::test]
    fn app_name_rejects_a_label_over_63_characters() {
        assert_eq!(
            teleport_app_name(
                CloudProvider::Generic,
                "northamerica-northeast2",
                ORG,
                "123",
            ),
            Err(
                "\"mz-generic-northamerica-northeast2-ahltbr22mfgxvhz4mpwqd42ox4-123\" \
                 is not a valid DNS-1035 label: longer than 63 characters"
                    .to_string()
            ),
        );
    }

    #[mz_ore::test]
    fn dns1035_label_error_matches_the_kubernetes_rules() {
        assert_eq!(dns1035_label_error("mz-aws-us-east-1-abc-0"), None);
        assert_eq!(
            dns1035_label_error("0-leading-digit"),
            Some("does not start with a lowercase letter"),
        );
        assert_eq!(
            dns1035_label_error("trailing-dash-"),
            Some("does not end with a lowercase letter or digit"),
        );
        assert_eq!(
            dns1035_label_error("mz-AWS-us-east-1"),
            Some("contains a character outside [-a-z0-9]"),
        );
    }
}
