---
source: src/ore/src/metrics/rule.rs
revision: c8a90af6a2
---

# mz-ore::metrics::rule

Declarative enrichment rules that attach human-readable name labels to Prometheus metrics at scrape time.

Subsystems register one or more `Rule` values alongside their metrics on a `MetricsRegistry`. When the federated `/metrics/public` endpoint scrapes a remote registry it resolves each rule against the catalog via a `NameLookup` implementation, appending resolved name labels to every metric in the targeted family without overwriting labels that are already present.

Key types:

* `Rule` — an enum with three variants that cover the common cases: `ClusterNameLookup` (reads a cluster ID label and writes the cluster name), `ReplicaNameLookup` (reads cluster and replica ID labels and writes the replica name), and `ObjectNameLookup` (reads a `GlobalId` label and writes the object name, schema name, and database name). The convenience constructor `Rule::object_name_lookup_with_default_labels` builds an `ObjectNameLookup` that writes to the conventional `schema_name` / `database_name` label names.
* `NameLookup` — trait implemented by any type that can resolve cluster, replica, and catalog-object IDs to their names; the production implementation queries the Materialize catalog.
* `ObjectName` — the resolved fully-qualified parts of a catalog object's name (`database`, `schema`, `object`).

`Rule::apply` iterates over every `Metric` in a `MetricFamily`, resolves the rule using the current labels, and appends only those resolved labels that are not already present, preventing duplicate or conflicting label pairs.
