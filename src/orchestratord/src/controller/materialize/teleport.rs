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
use mz_cloud_provider::CloudProvider;
use mz_cloud_resources::crd::materialize::v1alpha1::Materialize;
use serde::Serialize;
use uuid::Uuid;

use super::Config;

/// The named environmentd Service port to register. Required: without it,
/// the agent appends every port name to the resource name and registers
/// four apps instead of one.
const TELEPORT_APP_PORT: &str = "internal-http";

/// The internal HTTP endpoint is plain HTTP, not HTTPS. Set explicitly
/// rather than relying on Teleport's port-name heuristic to guess it.
const TELEPORT_APP_PROTOCOL: &str = "http";

const TELEPORT_APP_DESCRIPTION: &str = "Environmentd Internal HTTP API";

/// Forces the support database user on internal HTTP requests reaching the
/// registered app. This is a security control, not a convenience: it is
/// what stops a support session from picking a different database role.
const TELEPORT_REWRITE_HEADER_NAME: &str = "X-Materialize-User";
const TELEPORT_REWRITE_HEADER_VALUE: &str = "mz_support";

#[derive(Serialize)]
struct RewriteHeader {
    name: String,
    value: String,
}

#[derive(Serialize)]
struct Rewrite {
    headers: Vec<RewriteHeader>,
}

/// The Teleport `app` resource name: `mz-{cloud_provider}-{region}-{base32(org_uuid)}-{ordinal}`.
///
/// `mz-` is transitional. It lets this registration coexist with the `app`
/// `environment-controller` still creates under the un-prefixed name; Phase
/// 1 ends by renaming this registration back to the un-prefixed form. Add
/// the prefix only here, never in `Materialize::environment_id`, which
/// other consumers (the `--environment-id` flag, KMS key aliases) rely on
/// staying stable.
fn teleport_app_name(cloud_provider: CloudProvider, region: &str, environment_id: Uuid) -> String {
    format!(
        "mz-{}-{}-{}-0",
        cloud_provider,
        region,
        BASE32_NOPAD
            .encode(environment_id.as_bytes())
            .to_lowercase(),
    )
}

/// Adds the Teleport discovery labels and annotations to `service` when
/// `--teleport-endpoint` is set. A no-op otherwise, which is the rollback
/// path: absent the flag, this function changes nothing about the Service.
pub(super) fn apply_teleport_registration(
    config: &Config,
    mz: &Materialize,
    service: &mut Service,
) {
    if config.teleport_endpoint.is_none() {
        return;
    }
    let stack_type = config
        .teleport_stack_type
        .as_ref()
        .expect("--teleport-stack-type is required when --teleport-endpoint is set");
    let cluster_name = config
        .teleport_cluster_name
        .as_ref()
        .expect("--teleport-cluster-name is required when --teleport-endpoint is set");

    let labels = service.metadata.labels.get_or_insert_with(BTreeMap::new);
    labels.insert(
        "materialize.cloud/app".to_string(),
        mz.environmentd_app_name(),
    );
    labels.insert("stack-type".to_string(), stack_type.clone());
    labels.insert("cluster".to_string(), cluster_name.clone());

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
    annotations.insert(
        "teleport.dev/name".to_string(),
        teleport_app_name(
            config.cloud_provider,
            &config.region,
            mz.spec.environment_id,
        ),
    );
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

    // Matches `environment_id_base32_from_env` in the `cloud` repo
    // (`src/environment/src/util.rs`), which the same organization UUID and
    // region produce today for the un-prefixed name.
    #[test]
    fn app_name_matches_the_base32_convention() {
        assert_eq!(
            teleport_app_name(
                CloudProvider::Local,
                "kind",
                uuid!("01d730c7-5a61-4d7a-9f3c-63ed01f34ebf"),
            ),
            "mz-local-kind-ahltbr22mfgxvhz4mpwqd42ox4-0",
        );
    }
}
