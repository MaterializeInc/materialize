// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Enrichment rules for Prometheus metrics.
//!
//! Subsystems register [`Rule`]s alongside their metrics on a
//! [`crate::metrics::MetricsRegistry`]. When env's federated `/metrics/public`
//! endpoint scrapes a remote registry, it resolves the rules against the
//! catalog to attach human-readable name labels (e.g. `cluster_name`,
//! `source_name`) onto metrics.

use std::collections::BTreeMap;

use prometheus::proto::{LabelPair, MetricFamily};
use serde::{Deserialize, Serialize};

/// Resolves IDs to their names
pub trait NameLookup {
    /// Returns the name of the cluster with the given ID, if it exists.
    fn cluster_name(&self, cluster_id: &str) -> Option<String>;
    /// Returns the name of the replica with the given (cluster, replica) IDs.
    fn replica_name(&self, cluster_id: &str, replica_id: &str) -> Option<String>;
    /// Returns the name of the catalog object with the given global ID.
    fn object_name(&self, global_id: &str) -> Option<String>;
}

/// A declarative enrichment rule applied to a metric family at scrape time.
///
/// Each variant reads one or more ID labels already present on a metric and
/// adds **exactly one** resolved name label.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Rule {
    /// Reads the cluster ID from `cluster_id_label` and writes the resolved
    /// cluster name into `output_label`.
    ClusterNameLookup {
        /// Name of the label on the metric that carries the cluster ID.
        cluster_id_label: String,
        /// Name of the label to add with the resolved cluster name.
        output_label: String,
    },
    /// Reads the cluster ID from `cluster_id_label` and the replica ID from
    /// `replica_id_label`, then writes the resolved replica name into
    /// `output_label`.
    ReplicaNameLookup {
        /// Name of the label on the metric that carries the cluster ID.
        cluster_id_label: String,
        /// Name of the label on the metric that carries the replica ID.
        replica_id_label: String,
        /// Name of the label to add with the resolved replica name.
        output_label: String,
    },
    /// Reads a `GlobalId` from `object_id_label` and writes the resolved
    /// catalog object name into `output_label`.
    ObjectNameLookup {
        /// Name of the label on the metric that carries the global ID.
        object_id_label: String,
        /// Name of the label to add with the resolved object name.
        output_label: String,
    },
}

impl Rule {
    /// The label name this rule writes into.
    pub fn output_label(&self) -> &str {
        match self {
            Rule::ClusterNameLookup { output_label, .. }
            | Rule::ReplicaNameLookup { output_label, .. }
            | Rule::ObjectNameLookup { output_label, .. } => output_label,
        }
    }

    /// Applies the rule to every metric in `family`.
    ///
    pub fn apply<L: NameLookup>(&self, family: &mut MetricFamily, lookup: &L) {
        let output_label = self.output_label();
        for metric in family.mut_metric() {
            let labels: BTreeMap<&str, &str> = metric
                .get_label()
                .iter()
                .map(|l| (l.name(), l.value()))
                .collect();
            // No-op for a metric that already carries the output label.
            if labels.contains_key(output_label) {
                continue;
            }
            let resolved = match self {
                Rule::ClusterNameLookup {
                    cluster_id_label, ..
                } => labels
                    .get(cluster_id_label.as_str())
                    .copied()
                    .and_then(|cid| lookup.cluster_name(cid)),
                Rule::ReplicaNameLookup {
                    cluster_id_label,
                    replica_id_label,
                    ..
                } => match (
                    labels.get(cluster_id_label.as_str()).copied(),
                    labels.get(replica_id_label.as_str()).copied(),
                ) {
                    (Some(cid), Some(rid)) => lookup.replica_name(cid, rid),
                    _ => None,
                },
                Rule::ObjectNameLookup {
                    object_id_label, ..
                } => labels
                    .get(object_id_label.as_str())
                    .copied()
                    .and_then(|oid| lookup.object_name(oid)),
            };
            let Some(value) = resolved else { continue };
            let mut all = metric.take_label();
            let mut pair = LabelPair::default();
            pair.set_name(output_label.to_owned());
            pair.set_value(value);
            all.push(pair);
            metric.set_label(all);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use prometheus::proto::{Counter, Metric, MetricFamily, MetricType};

    use super::*;

    /// In-memory [`NameLookup`] for unit tests.
    #[derive(Default)]
    struct FakeCatalog {
        clusters: BTreeMap<String, String>,
        replicas: BTreeMap<(String, String), String>,
        objects: BTreeMap<String, String>,
    }

    impl FakeCatalog {
        fn with_cluster(mut self, id: &str, name: &str) -> Self {
            self.clusters.insert(id.into(), name.into());
            self
        }
        fn with_replica(mut self, cid: &str, rid: &str, name: &str) -> Self {
            self.replicas.insert((cid.into(), rid.into()), name.into());
            self
        }
        fn with_object(mut self, id: &str, name: &str) -> Self {
            self.objects.insert(id.into(), name.into());
            self
        }
    }

    impl NameLookup for FakeCatalog {
        fn cluster_name(&self, cluster_id: &str) -> Option<String> {
            self.clusters.get(cluster_id).cloned()
        }
        fn replica_name(&self, cluster_id: &str, replica_id: &str) -> Option<String> {
            self.replicas
                .get(&(cluster_id.to_owned(), replica_id.to_owned()))
                .cloned()
        }
        fn object_name(&self, global_id: &str) -> Option<String> {
            self.objects.get(global_id).cloned()
        }
    }

    fn label(name: &str, value: &str) -> LabelPair {
        let mut p = LabelPair::default();
        p.set_name(name.into());
        p.set_value(value.into());
        p
    }

    fn family_with_labels(labels: Vec<LabelPair>) -> MetricFamily {
        let mut family = MetricFamily::default();
        family.set_name("test_metric".into());
        family.set_field_type(MetricType::COUNTER);
        family.set_help("help for test_metric".into());
        let mut metric = Metric::default();
        let mut counter = Counter::default();
        counter.set_value(1.0);
        metric.set_counter(counter);
        metric.set_label(labels);
        family.set_metric(vec![metric]);
        family
    }

    fn label_names(family: &MetricFamily) -> Vec<&str> {
        family.get_metric()[0]
            .get_label()
            .iter()
            .map(|l| l.name())
            .collect()
    }

    fn label_value<'a>(family: &'a MetricFamily, name: &str) -> Option<&'a str> {
        family.get_metric()[0]
            .get_label()
            .iter()
            .find(|l| l.name() == name)
            .map(|l| l.value())
    }

    #[crate::test]
    fn cluster_name_lookup_attaches_name() {
        let mut family = family_with_labels(vec![label("cluster_id", "u1")]);
        let catalog = FakeCatalog::default().with_cluster("u1", "quickstart");
        let rule = Rule::ClusterNameLookup {
            cluster_id_label: "cluster_id".into(),
            output_label: "cluster_name".into(),
        };
        rule.apply(&mut family, &catalog);
        assert_eq!(label_names(&family), vec!["cluster_id", "cluster_name"]);
        assert_eq!(label_value(&family, "cluster_name"), Some("quickstart"));
    }

    #[crate::test]
    fn replica_name_lookup_attaches_name() {
        let mut family =
            family_with_labels(vec![label("cluster_id", "u1"), label("replica_id", "u2")]);
        let catalog = FakeCatalog::default().with_replica("u1", "u2", "r1");
        let rule = Rule::ReplicaNameLookup {
            cluster_id_label: "cluster_id".into(),
            replica_id_label: "replica_id".into(),
            output_label: "replica_name".into(),
        };
        rule.apply(&mut family, &catalog);
        assert_eq!(
            label_names(&family),
            vec!["cluster_id", "replica_id", "replica_name"]
        );
        assert_eq!(label_value(&family, "replica_name"), Some("r1"));
    }

    #[crate::test]
    fn object_name_lookup_attaches_name_with_custom_output() {
        let mut family = family_with_labels(vec![label("collection_id", "s100")]);
        let catalog = FakeCatalog::default().with_object("s100", "my_source");
        let rule = Rule::ObjectNameLookup {
            object_id_label: "collection_id".into(),
            output_label: "source_name".into(),
        };
        rule.apply(&mut family, &catalog);
        assert_eq!(label_names(&family), vec!["collection_id", "source_name"]);
        assert_eq!(label_value(&family, "source_name"), Some("my_source"));
    }

    #[crate::test]
    fn apply_skips_when_input_label_missing() {
        let mut family = family_with_labels(vec![label("unrelated", "x")]);
        let catalog = FakeCatalog::default().with_cluster("u1", "quickstart");
        let rule = Rule::ClusterNameLookup {
            cluster_id_label: "cluster_id".into(),
            output_label: "cluster_name".into(),
        };
        rule.apply(&mut family, &catalog);
        assert_eq!(label_names(&family), vec!["unrelated"]);
    }

    #[crate::test]
    fn apply_skips_when_lookup_returns_none() {
        let mut family = family_with_labels(vec![label("cluster_id", "u99")]);
        let catalog = FakeCatalog::default(); // no clusters
        let rule = Rule::ClusterNameLookup {
            cluster_id_label: "cluster_id".into(),
            output_label: "cluster_name".into(),
        };
        rule.apply(&mut family, &catalog);
        assert_eq!(label_names(&family), vec!["cluster_id"]);
    }

    #[crate::test]
    fn apply_skips_when_output_label_already_present() {
        let mut family = family_with_labels(vec![
            label("cluster_id", "u1"),
            label("cluster_name", "preset"),
        ]);
        let catalog = FakeCatalog::default().with_cluster("u1", "quickstart");
        let rule = Rule::ClusterNameLookup {
            cluster_id_label: "cluster_id".into(),
            output_label: "cluster_name".into(),
        };
        rule.apply(&mut family, &catalog);
        // Existing value preserved; not overwritten, not duplicated.
        assert_eq!(label_names(&family), vec!["cluster_id", "cluster_name"]);
        assert_eq!(label_value(&family, "cluster_name"), Some("preset"));
    }

    #[crate::test]
    fn two_rules() {
        let mut family =
            family_with_labels(vec![label("cluster_id", "u1"), label("replica_id", "u2")]);
        let catalog = FakeCatalog::default()
            .with_cluster("u1", "quickstart")
            .with_replica("u1", "u2", "r1");
        let cluster_rule = Rule::ClusterNameLookup {
            cluster_id_label: "cluster_id".into(),
            output_label: "cluster_name".into(),
        };
        let replica_rule = Rule::ReplicaNameLookup {
            cluster_id_label: "cluster_id".into(),
            replica_id_label: "replica_id".into(),
            output_label: "replica_name".into(),
        };
        cluster_rule.apply(&mut family, &catalog);
        replica_rule.apply(&mut family, &catalog);
        assert_eq!(label_value(&family, "cluster_name"), Some("quickstart"));
        assert_eq!(label_value(&family, "replica_name"), Some("r1"));
    }
}
