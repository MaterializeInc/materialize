// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The `MaterializeDebug` custom resource: an in-cluster debug collector for
//! one Materialize instance.

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::api::core::v1::ResourceRequirements;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::{CustomResource, Resource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::crd::{ManagedResource, new_resource_id};

/// Default for [`SnapshotInterval`]. Half an hour keeps the buffer's
/// history at several hours with the default retention while keeping the
/// per-snapshot load on the instance (catalog queries, heap dumps)
/// infrequent.
pub const DEFAULT_SNAPSHOT_INTERVAL: &str = "30m";
/// Default for `retainedSnapshots`.
pub const DEFAULT_RETAINED_SNAPSHOTS: u32 = 12;
/// Default for `bufferSizeLimit`.
pub const DEFAULT_BUFFER_SIZE_LIMIT: &str = "2Gi";
/// Default for `cpuProfileDurationSeconds`.
pub const DEFAULT_CPU_PROFILE_DURATION_SECONDS: u64 = 10;

/// How often the collector takes a periodic snapshot, as a human-readable
/// duration such as `30m` or `1h 30m`.
///
/// A transparent wrapper whose [`Default`] is [`DEFAULT_SNAPSHOT_INTERVAL`],
/// so the derived `Default` of the spec, serde's `#[serde(default)]` and the
/// schema default in the generated CRD all agree.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct SnapshotInterval(pub String);

impl Default for SnapshotInterval {
    fn default() -> Self {
        SnapshotInterval(DEFAULT_SNAPSHOT_INTERVAL.to_owned())
    }
}

/// Parses a Kubernetes resource quantity into a number of bytes.
///
/// Handles the binary (`Ki`, `Mi`, ...), decimal (`k`, `M`, ...) and
/// exponent (`e3`) suffixes plus fractional mantissas, using integer
/// arithmetic so `1.5Gi` is exact. Sub-byte results round down. Returns
/// `None` for anything that is not a quantity.
pub fn parse_quantity_bytes(quantity: &str) -> Option<u64> {
    let quantity = quantity.trim();
    let split = quantity
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(quantity.len());
    let (mantissa, suffix) = quantity.split_at(split);
    if mantissa.is_empty() {
        return None;
    }

    // mantissa = digits / 10^scale
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    let digits: u128 = format!("{int_part}{frac_part}").parse().ok()?;
    let scale: u32 = u32::try_from(frac_part.len()).ok()?;

    let (numerator, denominator): (u128, u128) = match suffix {
        "" => (1, 1),
        "Ki" => (1u128 << 10, 1),
        "Mi" => (1u128 << 20, 1),
        "Gi" => (1u128 << 30, 1),
        "Ti" => (1u128 << 40, 1),
        "Pi" => (1u128 << 50, 1),
        "Ei" => (1u128 << 60, 1),
        "k" => (10u128.pow(3), 1),
        "M" => (10u128.pow(6), 1),
        "G" => (10u128.pow(9), 1),
        "T" => (10u128.pow(12), 1),
        "P" => (10u128.pow(15), 1),
        "E" => (10u128.pow(18), 1),
        "m" => (1, 1000),
        exponent => {
            let exponent = exponent
                .strip_prefix('e')
                .or_else(|| exponent.strip_prefix('E'))?;
            let (negative, magnitude) = match exponent.strip_prefix('-') {
                Some(magnitude) => (true, magnitude),
                None => (false, exponent.strip_prefix('+').unwrap_or(exponent)),
            };
            let magnitude: u32 = magnitude.parse().ok()?;
            if magnitude > 38 {
                return None;
            }
            if negative {
                (1, 10u128.pow(magnitude))
            } else {
                (10u128.pow(magnitude), 1)
            }
        }
    };

    let bytes = digits
        .checked_mul(numerator)?
        .checked_div(denominator.checked_mul(10u128.checked_pow(scale)?)?)?;
    u64::try_from(bytes).ok()
}

pub mod v1alpha1 {
    use super::*;

    /// Which categories a periodic snapshot collects.
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
    #[serde(rename_all = "camelCase", default)]
    pub struct DebugCollectionConfig {
        /// Kubernetes resources, their describe output, and pod logs.
        pub k8s: bool,
        /// System catalog relations, as CSV.
        pub system_catalog: bool,
        /// Heap profiles of environmentd and every clusterd process.
        pub heap_profiles: bool,
        /// Prometheus metrics of environmentd and every clusterd process.
        pub prometheus_metrics: bool,
        /// CPU profiles of environmentd and every clusterd process. Off by
        /// default: a capture disables memory profiling on the process for
        /// its duration and adds sampling load, which is not worth paying
        /// every interval unasked. Snapshots requested through the CLI
        /// include CPU profiles regardless of this setting.
        pub cpu_profiles: bool,
        /// How long each CPU profile samples for.
        pub cpu_profile_duration_seconds: u64,
    }

    impl Default for DebugCollectionConfig {
        fn default() -> Self {
            Self {
                k8s: true,
                system_catalog: true,
                heap_profiles: true,
                prometheus_metrics: true,
                cpu_profiles: false,
                cpu_profile_duration_seconds: DEFAULT_CPU_PROFILE_DURATION_SECONDS,
            }
        }
    }

    #[derive(
        CustomResource,
        Clone,
        Debug,
        Default,
        PartialEq,
        Deserialize,
        Serialize,
        JsonSchema
    )]
    #[serde(rename_all = "camelCase")]
    #[kube(
        namespaced,
        group = "materialize.cloud",
        version = "v1alpha1",
        kind = "MaterializeDebug",
        singular = "materializedebug",
        plural = "materializedebugs",
        shortname = "mzdbg",
        status = "MaterializeDebugStatus",
        printcolumn = r#"{"name": "Materialize", "type": "string", "description": "The Materialize instance being collected from.", "jsonPath": ".spec.materializeName"}"#,
        printcolumn = r#"{"name": "Ready", "type": "string", "description": "Whether the collector deployment is ready", "jsonPath": ".status.conditions[?(@.type==\"Ready\")].status"}"#,
        printcolumn = r#"{"name": "ImageRef", "type": "string", "description": "Reference to the collector image.", "jsonPath": ".spec.collectorImageRef", "priority": 1}"#
    )]
    pub struct MaterializeDebugSpec {
        /// The name of the Materialize resource, in the same namespace, to
        /// collect diagnostics from.
        pub materialize_name: String,
        /// The mz-debug image to run. Defaults to the `mz-debug` image with
        /// the same registry and tag as the instance's environmentd image.
        pub collector_image_ref: Option<String>,
        /// How often to take a periodic snapshot, as a human-readable
        /// duration such as `30m` or `1h 30m`. Defaults to `30m`; an
        /// unparseable value also falls back to that default.
        #[serde(default)]
        pub snapshot_interval: SnapshotInterval,
        /// How many snapshots to keep. The oldest are evicted first.
        /// Defaults to 12.
        pub retained_snapshots: Option<u32>,
        /// The total size of retained snapshots above which the oldest are
        /// evicted. Defaults to `2Gi`. The collector's buffer volume is sized
        /// somewhat above this to leave room for the snapshot being taken.
        pub buffer_size_limit: Option<Quantity>,
        /// Which categories periodic snapshots collect.
        #[serde(default)]
        pub collect: DebugCollectionConfig,
        /// Namespaces other than the instance's whose resources and pod logs
        /// are included in snapshots, for example the operator's namespace.
        pub additional_namespaces: Option<Vec<String>>,
        /// Resource requirements for the collector pod.
        pub resource_requirements: Option<ResourceRequirements>,
        /// Annotations to apply to the collector pod.
        pub pod_annotations: Option<BTreeMap<String, String>>,
        /// Labels to apply to the collector pod.
        pub pod_labels: Option<BTreeMap<String, String>>,
        /// Overrides the randomly chosen resource id. The operator sets this
        /// to the instance's resource id so the collector pod is covered by
        /// the instance's network policies.
        pub resource_id: Option<String>,
    }

    impl MaterializeDebug {
        pub fn name_prefixed(&self, suffix: &str) -> String {
            format!("mz{}-{}", self.resource_id(), suffix)
        }

        pub fn resource_id(&self) -> &str {
            &self.status.as_ref().unwrap().resource_id
        }

        pub fn namespace(&self) -> String {
            self.meta().namespace.clone().unwrap()
        }

        pub fn app_name(&self) -> String {
            "debug-collector".to_owned()
        }

        pub fn deployment_name(&self) -> String {
            self.name_prefixed("debug-collector")
        }

        pub fn service_name(&self) -> String {
            self.name_prefixed("debug-collector")
        }

        pub fn service_account_name(&self) -> String {
            self.name_prefixed("debug-collector")
        }

        /// The RoleBinding granting the collector its namespaced permissions
        /// in `namespace`. The owning namespace is part of the name because
        /// bindings for `additionalNamespaces` live in foreign namespaces,
        /// where collectors of several instances may coexist.
        pub fn role_binding_name(&self, namespace: &str) -> String {
            if namespace == self.namespace() {
                self.name_prefixed("debug-collector")
            } else {
                self.name_prefixed(&format!("debug-collector-{}", self.namespace()))
            }
        }

        /// The ClusterRoleBinding granting the collector its cluster-scoped
        /// permissions. Cluster-unique, hence the namespace suffix.
        pub fn cluster_role_binding_name(&self) -> String {
            self.name_prefixed(&format!("debug-collector-{}", self.namespace()))
        }

        /// Namespaces whose resources are collected: the instance's own plus
        /// `additionalNamespaces`, deduplicated.
        pub fn collected_namespaces(&self) -> Vec<String> {
            let mut namespaces = vec![self.namespace()];
            for namespace in self.spec.additional_namespaces.iter().flatten() {
                if !namespaces.contains(namespace) {
                    namespaces.push(namespace.clone());
                }
            }
            namespaces
        }

        /// The snapshot interval, falling back to
        /// [`DEFAULT_SNAPSHOT_INTERVAL`] when unparseable.
        pub fn snapshot_interval(&self) -> Duration {
            let interval = &self.spec.snapshot_interval.0;
            humantime::parse_duration(interval)
                .or_else(|e| {
                    tracing::warn!(
                        snapshot_interval = %interval,
                        "failed to parse snapshotInterval, using default: {e}",
                    );
                    humantime::parse_duration(DEFAULT_SNAPSHOT_INTERVAL)
                })
                .expect("DEFAULT_SNAPSHOT_INTERVAL must be a valid duration")
        }

        pub fn retained_snapshots(&self) -> u32 {
            self.spec
                .retained_snapshots
                .unwrap_or(DEFAULT_RETAINED_SNAPSHOTS)
        }

        /// The buffer size cap in bytes, falling back to
        /// [`DEFAULT_BUFFER_SIZE_LIMIT`] when unparseable.
        pub fn buffer_size_limit_bytes(&self) -> u64 {
            let default = || {
                parse_quantity_bytes(DEFAULT_BUFFER_SIZE_LIMIT)
                    .expect("DEFAULT_BUFFER_SIZE_LIMIT must be a valid quantity")
            };
            match &self.spec.buffer_size_limit {
                None => default(),
                Some(quantity) => parse_quantity_bytes(&quantity.0).unwrap_or_else(|| {
                    tracing::warn!(
                        buffer_size_limit = %quantity.0,
                        "failed to parse bufferSizeLimit, using default",
                    );
                    default()
                }),
            }
        }

        /// The size limit of the collector's buffer volume: the cap plus a
        /// tenth, since the snapshot being taken sits next to the retained
        /// ones until it is committed and the oldest evicted.
        pub fn buffer_volume_size_limit(&self) -> Quantity {
            let bytes = self.buffer_size_limit_bytes();
            Quantity(format!("{}", bytes + bytes / 10))
        }

        pub fn status(&self) -> MaterializeDebugStatus {
            self.status
                .clone()
                .unwrap_or_else(|| MaterializeDebugStatus {
                    resource_id: self
                        .spec
                        .resource_id
                        .clone()
                        .unwrap_or_else(new_resource_id),
                    conditions: vec![],
                })
        }
    }

    #[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct MaterializeDebugStatus {
        /// Resource identifier used as a name prefix to avoid pod name collisions.
        pub resource_id: String,

        pub conditions: Vec<Condition>,
    }

    impl ManagedResource for MaterializeDebug {
        fn default_labels(&self) -> BTreeMap<String, String> {
            BTreeMap::from_iter([
                (
                    "materialize.cloud/organization-name".to_owned(),
                    self.spec.materialize_name.clone(),
                ),
                (
                    "materialize.cloud/organization-namespace".to_owned(),
                    self.namespace(),
                ),
                (
                    "materialize.cloud/mz-resource-id".to_owned(),
                    self.resource_id().to_owned(),
                ),
                ("materialize.cloud/app".to_owned(), self.app_name()),
            ])
        }

        fn app_name(&self) -> Option<&str> {
            Some("debug-collector")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kube::core::ObjectMeta;

    use super::v1alpha1::{MaterializeDebug, MaterializeDebugSpec, MaterializeDebugStatus};
    use super::*;

    fn debug(spec: MaterializeDebugSpec) -> MaterializeDebug {
        MaterializeDebug {
            metadata: ObjectMeta {
                name: Some("mz".to_owned()),
                namespace: Some("materialize-environment".to_owned()),
                ..Default::default()
            },
            spec,
            status: Some(MaterializeDebugStatus {
                resource_id: "abcdef0123".to_owned(),
                conditions: vec![],
            }),
        }
    }

    #[mz_ore::test]
    fn parse_quantity_bytes_handles_kubernetes_suffixes() {
        for (input, expected) in [
            ("2Gi", Some(2u64 << 30)),
            ("1.5Gi", Some(3u64 << 29)),
            ("512Mi", Some(512 << 20)),
            ("1Ki", Some(1024)),
            ("2G", Some(2_000_000_000)),
            ("500M", Some(500_000_000)),
            ("1k", Some(1000)),
            ("1000", Some(1000)),
            ("1e9", Some(1_000_000_000)),
            ("2E3", Some(2000)),
            ("1500m", Some(1)),
            (" 2Gi ", Some(2u64 << 30)),
            ("0.5", Some(0)),
            ("", None),
            ("Gi", None),
            ("2Gib", None),
            ("-1Gi", None),
            ("1.2.3Gi", None),
            ("999999999999999999999999999Ei", None),
        ] {
            assert_eq!(parse_quantity_bytes(input), expected, "input: {input:?}");
        }
    }

    #[mz_ore::test]
    fn minimal_spec_takes_defaults() {
        let spec: MaterializeDebugSpec =
            serde_json::from_value(serde_json::json!({"materializeName": "mz"})).unwrap();
        assert_eq!(spec.snapshot_interval.0, DEFAULT_SNAPSHOT_INTERVAL);
        assert!(spec.collect.k8s && spec.collect.system_catalog);
        assert!(!spec.collect.cpu_profiles);
        assert_eq!(
            spec.collect.cpu_profile_duration_seconds,
            DEFAULT_CPU_PROFILE_DURATION_SECONDS
        );

        let debug = debug(spec);
        assert_eq!(debug.snapshot_interval(), Duration::from_secs(30 * 60));
        assert_eq!(debug.retained_snapshots(), DEFAULT_RETAINED_SNAPSHOTS);
        assert_eq!(debug.buffer_size_limit_bytes(), 2u64 << 30);
        assert_eq!(
            debug.buffer_volume_size_limit(),
            Quantity(((2u64 << 30) + (2u64 << 30) / 10).to_string())
        );
        assert_eq!(debug.deployment_name(), "mzabcdef0123-debug-collector");
        assert_eq!(
            debug.role_binding_name("materialize-environment"),
            "mzabcdef0123-debug-collector"
        );
        assert_eq!(
            debug.role_binding_name("materialize"),
            "mzabcdef0123-debug-collector-materialize-environment"
        );
        assert_eq!(
            debug.cluster_role_binding_name(),
            "mzabcdef0123-debug-collector-materialize-environment"
        );
        assert_eq!(
            debug.collected_namespaces(),
            vec!["materialize-environment".to_owned()]
        );
    }

    #[mz_ore::test]
    fn unparseable_values_fall_back_to_defaults() {
        let debug = debug(MaterializeDebugSpec {
            materialize_name: "mz".to_owned(),
            snapshot_interval: SnapshotInterval("soon".to_owned()),
            buffer_size_limit: Some(Quantity("lots".to_owned())),
            additional_namespaces: Some(vec![
                "materialize".to_owned(),
                "materialize-environment".to_owned(),
            ]),
            ..Default::default()
        });
        assert_eq!(debug.snapshot_interval(), Duration::from_secs(30 * 60));
        assert_eq!(debug.buffer_size_limit_bytes(), 2u64 << 30);
        assert_eq!(
            debug.collected_namespaces(),
            vec![
                "materialize-environment".to_owned(),
                "materialize".to_owned()
            ]
        );
    }

    #[mz_ore::test]
    fn schema_carries_defaults() {
        // The defaults must be in the generated CRD's OpenAPI schema, not
        // just in the Rust helpers, so the API server fills them in and
        // `kubectl explain` shows them.
        let crd = serde_json::to_value(<MaterializeDebug as kube::CustomResourceExt>::crd())
            .expect("CRD serializes");
        let spec_schema = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"];
        assert_eq!(
            spec_schema["snapshotInterval"]["default"],
            serde_json::json!(DEFAULT_SNAPSHOT_INTERVAL)
        );
        assert_eq!(
            spec_schema["collect"]["default"]["cpuProfiles"],
            serde_json::json!(false)
        );
        assert_eq!(
            spec_schema["collect"]["default"]["cpuProfileDurationSeconds"],
            serde_json::json!(DEFAULT_CPU_PROFILE_DURATION_SECONDS)
        );
        assert_eq!(
            crd["spec"]["names"]["shortNames"],
            serde_json::json!(["mzdbg"])
        );
    }
}
