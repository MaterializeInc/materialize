---
title: "Configuring System Parameters"
description: "How to configure system parameters for Materialize using a Kubernetes ConfigMap"
aliases:
  - /self-managed/configuration-system-parameters/
menu:
  main:
    parent: "sm-deployments"
    weight: 71
---

This guide explains how to configure system parameters for your Materialize
deployment using a Kubernetes ConfigMap.

## Overview

System parameters allow you to customize the behavior of your Materialize
instance at runtime. These parameters can control various aspects such as
connection limits, cluster replica sizes, and other operational settings.

There are two ways to configure system parameters:

- **Using SQL**: Connect to your Materialize instance and use the [`ALTER SYSTEM
  SET`](/sql/alter-system-set/) command to modify parameters dynamically. This
  is useful for one-off changes or testing.

- **Using a ConfigMap**: Create a Kubernetes ConfigMap containing the parameters
  in JSON format and reference it in your Materialize custom resource. This is
  the recommended approach for persistent configuration that survives restarts
  and upgrades.

This guide focuses on the ConfigMap approach for self-managed deployments.

{{< public-preview />}}

## Configure System Parameters via ConfigMap

### Step 1: Create a System Parameters ConfigMap

In the same namespace as your Materialize environment, create a
ConfigMap that includes a key named `system-params.json`. Set
`system-params.json` to a valid JSON object containing your desired system
parameters.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mz-system-params
  namespace: materialize-environment
data:
  system-params.json: |
    {
      "max_connections": 1000,
      "allowed_cluster_replica_sizes": "'25cc', '50cc', '100cc'"
    }
```

Each top-level key sets a parameter for the whole environment. To set a
parameter for a subset of your clusters or replicas instead, see [Scoping
Parameters to Clusters or
Replicas](#scoping-parameters-to-clusters-or-replicas).

Apply the ConfigMap to your cluster:

```shell
kubectl apply -f system-params-configmap.yaml
```

### Step 2: Configure the Materialize Custom Resource

Reference the ConfigMap in your Materialize custom resource by setting the
`systemParameterConfigmapName` field to the name of your ConfigMap:

{{< tabs >}}
{{< tab "v1alpha1" >}}

{{< self-managed/crd-version-note "v1alpha1" >}}

```yaml {hl_lines="9-10"}
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:{{< self-managed/versions/get-latest-version >}}
  backendSecretName: materialize-backend
  systemParameterConfigmapName: mz-system-params
  requestRollout: 00000000-0000-0000-0000-000000000003 # Changing the CR requires a rollout
```

{{< /tab >}}
{{< tab "v1" >}}

{{< self-managed/crd-version-note "v1" >}}

```yaml {hl_lines="9"}
apiVersion: materialize.cloud/v1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:{{< self-managed/versions/get-latest-version >}}
  backendSecretName: materialize-backend
  systemParameterConfigmapName: mz-system-params
```

{{< /tab >}}
{{< /tabs >}}

Apply the updated Materialize resource:

```shell
kubectl apply -f materialize.yaml
```

## Updating ConfigMap System Parameters

To update system parameters defined in your ConfigMap, you can either:

- Use `kubectl edit configmap` to edit the ConfigMap and apply the changes:

  ```shell
  kubectl edit configmap mz-system-params -n materialize-environment
  ```

- Or, edit the ConfigMap YAML file and reapply:

  ```shell
  kubectl apply -f system-params-configmap.yaml
  ```

Unlike changes to the Materialize custom resource, updating the parameters in
your ConfigMap does **not** require a rollout.

### ConfigMap sync behavior

Kubernetes uses the kubelet to periodically sync ConfigMap updates to mounted
volumes. By default, this sync occurs approximately every 60 seconds. This
means changes to your ConfigMap may take up to a minute to be reflected in
the running Materialize instance.

Once the ConfigMap is synced to the volume, Materialize checks for configuration
changes every second and applies them automatically.

To force an immediate sync of the ConfigMap from Kubernetes, you can update an
annotation on the Materialize resource, which triggers a pod re-reconciliation:

```shell
kubectl annotate materialize <instance-name> \
  -n materialize-environment \
  configmap-reload-trigger="$(date +%s)" \
  --overwrite
```

Alternatively, you can add the `configmap-reload-trigger` annotation to your
Materialize custom resource YAML and update it whenever you need to force a
ConfigMap reload:

{{< tabs >}}
{{< tab "v1alpha1" >}}

{{< self-managed/crd-version-note "v1alpha1" >}}

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
  annotations:
    configmap-reload-trigger: "1234567890"  # Update this value to force reload
spec:
  # ... rest of spec
```

{{< /tab >}}
{{< tab "v1" >}}

{{< self-managed/crd-version-note "v1" >}}

```yaml
apiVersion: materialize.cloud/v1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
  annotations:
    configmap-reload-trigger: "1234567890"  # Update this value to force reload
spec:
  # ... rest of spec
```

{{< /tab >}}
{{< /tabs >}}

{{< note >}}
Even after the ConfigMap is synced, some system parameters may require a restart to
take effect.
{{< /note >}}

## Scoping Parameters to Clusters or Replicas

Every top-level key in `system-params.json` sets a parameter for the whole
environment, with two exceptions: `segments` and `rules` are reserved keys that
together scope parameters to a subset of your clusters and replicas.

- A **segment** is a named predicate over the attributes of a cluster or replica,
  for example "every replica of a legacy size family".
- A **rule** attaches parameters to a segment. The rules are an ordered array,
  and for each parameter the first matching rule wins.

For example, the following configuration sets environment-wide parameters and
overrides parameters for one cluster and for a class of replicas:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mz-system-params
  namespace: materialize-environment
data:
  system-params.json: |
    {
      "max_connections": 1000,
      "enable_lgalloc": true,

      "segments": {
        "analytics-cluster": { "cluster_name": ["analytics"] },
        "legacy-replicas": { "replica_size_family": ["legacy"] }
      },

      "rules": [
        {
          "segment": "analytics-cluster",
          "parameters": { "enable_eager_delta_joins": true }
        },
        {
          "segment": "legacy-replicas",
          "parameters": { "enable_lgalloc": false }
        }
      ]
    }
```

In this example, `max_connections` and `enable_lgalloc` apply environment-wide,
the `analytics` cluster additionally enables `enable_eager_delta_joins`, and
every replica of a legacy size family turns `enable_lgalloc` back off for itself.

A ConfigMap that uses neither reserved key is a plain environment-wide parameter
map. No system parameter is named `segments` or `rules`, so a flat ConfigMap can
never be reinterpreted as a scoped one.

### Segments

A segment maps an attribute name to the list of values it allows:

```json
"segments": {
  "legacy-in-analytics": {
    "cluster_name": ["analytics", "analytics_staging"],
    "replica_size_family": ["legacy"]
  }
}
```

- Several values for one attribute are **ORed**: the cluster name may be either
  `analytics` or `analytics_staging`.
- Several attributes in one segment are **ANDed**: the replica must be in one of
  those clusters *and* be of the `legacy` size family.
- **Only exact matching is supported.** There is no prefix, wildcard, regular
  expression, or negation operator. To target one cluster, write a segment with a
  single `cluster_name` value.
- A segment with an empty predicate, `{}`, matches every cluster and replica.
  Combined with rule ordering, that makes it a catch-all.

The available attributes are:

| Attribute             | Applies to         | Example        |
| --------------------- | ------------------ | -------------- |
| `cluster_name`        | clusters, replicas | `"quickstart"` |
| `cluster_id`          | clusters, replicas | `"u1"`         |
| `is_builtin`          | clusters, replicas | `true`         |
| `replica_name`        | replicas only      | `"r1"`         |
| `replica_id`          | replicas only      | `"u2"`         |
| `replica_size`        | replicas only      | `"25cc"`       |
| `replica_size_family` | replicas only      | `"legacy"`     |

`is_builtin` is `true` for a system cluster such as `mz_catalog_server`, and for
its replicas.

A replica carries its owning cluster's attributes, so a segment written with
`cluster_name` alone selects every replica of that cluster. A cluster carries no
replica attributes, so a segment mentioning any of them selects no cluster.

Prefer the name attributes over the id ones. An id identifies one incarnation of
an object: drop and recreate a cluster and its id changes, so a segment written
against the old id stops matching. A segment written against a name re-applies to
any cluster or replica later created with that name.

Values may be written as JSON strings, numbers, or booleans: `"is_builtin":
[true]` and `"is_builtin": ["true"]` are equivalent.

### Rules

Each element of `rules` names a segment and the parameters to apply to the
objects that segment matches:

```json
"rules": [
  { "segment": "legacy-in-analytics", "parameters": { "enable_lgalloc": false } },
  { "segment": "everything", "parameters": { "enable_lgalloc": true } }
]
```

- **The first matching rule wins**, per object and per parameter. Order the rules
  from the most specific to the most general.
- A rule that does not mention a parameter does not affect it, so a later rule
  may still set it.
- A parameter no matching rule sets falls back to the environment-wide value,
  that is, to the top-level key or to the parameter's default.

`rules` is an array rather than an object because the order is part of the
configuration, and the order of an object's keys is not preserved.

### Behavior worth knowing

- **Not every parameter can be scoped.** Only parameters whose scope is
  `cluster` or `replica` can be set in a rule. Any other parameter is ignored
  there, so set it as a top-level key instead. `environmentd` logs a warning
  naming the parameter and the rule, which also catches a misspelled parameter
  name.
- **A cluster-scoped parameter cannot be attached to a replica segment.** A
  cluster-scoped parameter, for example an optimizer feature, is consumed once
  per cluster when a dataflow is planned, so it must resolve identically for
  every replica of that cluster. A rule that supplies one through a segment
  matching on a replica attribute has that parameter dropped, with a warning
  naming the segment, the parameter, and the offending attribute. Replica-scoped
  parameters can be attached to either kind of segment.
- **A segment that Materialize cannot fully interpret matches nothing.** An
  unknown attribute name, or a value that is not a list of scalars, makes the
  whole segment match no cluster and no replica, so the rules naming it do not
  apply. This fails safe: ignoring the entry instead would widen the segment to
  objects you did not target. `environmentd` logs a warning naming the segment
  and the attribute.
- **A rule naming a segment that does not exist is ignored**, with a warning
  naming the segment.
- **A segment matching nothing is not an error.** If you later create a cluster
  or replica the segment matches, the rules apply to it.
- **A value that matches the environment-wide value records no override.**
  Overrides are only stored where they actually differ.
- **An unparseable value is dropped, not rejected.** The rest of the file still
  applies and `environmentd` logs a warning naming the parameter and the rule.
- **Removing a rule or segment removes the override**, returning the affected
  objects to the environment-wide value.
- **A ConfigMap that cannot be read or parsed leaves the existing overrides in
  place.** A malformed JSON document, or a deleted ConfigMap, carries no
  information rather than an empty set of overrides, so nothing is removed until a
  readable ConfigMap says so. Removing an override means editing the file, not
  breaking it.

To see which overrides are currently in effect, query:

```sql
SELECT c.name AS cluster, p.name, p.value
FROM mz_internal.mz_cluster_system_parameters p
JOIN mz_clusters c ON c.id = p.cluster_id;

SELECT c.name AS cluster, r.name AS replica, p.name, p.value
FROM mz_internal.mz_replica_system_parameters p
JOIN mz_cluster_replicas r ON r.id = p.replica_id
JOIN mz_clusters c ON c.id = r.cluster_id;
```

## Available System Parameters

The system parameters that can be configured via the ConfigMap are the same
parameters that can be modified using the [`ALTER SYSTEM SET`](/sql/alter-system-set/)
SQL command.

The following are some commonly configured system parameters:

| Parameter | Description |
|-----------|-------------|
| `max_connections` | Maximum number of concurrent connections allowed |
| `allowed_cluster_replica_sizes` | List of allowed cluster replica sizes |
| `max_clusters` | Maximum number of clusters in the region |
| `max_sources` | Maximum number of sources in the region |
| `max_sinks` | Maximum number of sinks in the region |

For a complete list of available system parameters and their descriptions, see
the [configuration parameters](/sql/alter-system-set/#key-configuration-parameters)
documentation, or run the following SQL command in your Materialize instance:

```sql
SHOW ALL;
```

### Sample ConfigMap: Setting Connection Limits

The following sample ConfigMap YAML sets the `max_connections` parameter:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mz-system-params
  namespace: materialize-environment
data:
  system-params.json: |
    {
      "max_connections": 500
    }
```

### Sample ConfigMap: Configuring Allowed Cluster Sizes

The following sample ConfigMap YAML sets the `allowed_cluster_replica_sizes` parameter:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mz-system-params
  namespace: materialize-environment
data:
  system-params.json: |
    {
      "allowed_cluster_replica_sizes": "'25cc', '50cc', '100cc', '200cc'"
    }
```

### Sample ConfigMap: Configuring Connection Limits and Allowed Cluster Sizes

The following sample ConfigMap YAML sets both the `max_connections` parameter
and the `allowed_cluster_replica_sizes` parameter:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mz-system-params
  namespace: materialize-environment
data:
  system-params.json: |
    {
      "max_connections": 500,
      "allowed_cluster_replica_sizes": "'25cc', '50cc', '100cc', '200cc'"
    }
```

## Troubleshooting

### ConfigMap not being applied

If your system parameters are not being applied, check the following:

1. **Verify the ConfigMap exists** in the correct namespace:
   ```shell
   kubectl get configmap mz-system-params -n materialize-environment
   ```

2. **Check the ConfigMap content** is valid JSON:
   ```shell
   kubectl get configmap mz-system-params -n materialize-environment -o jsonpath='{.data.system-params\.json}'
   ```

3. **Verify the Materialize resource** references the correct ConfigMap name:
   ```shell
   kubectl get materialize -n materialize-environment -o yaml | grep systemParameterConfigmapName
   ```

4. **Check environmentd logs** for any errors related to configuration loading:
   ```shell
   kubectl logs -l app=environmentd -n materialize-environment
   ```

### Invalid parameter values

If a system parameter value is invalid, Materialize will log an error but
continue running with the previous valid configuration. Check the environmentd
logs for error messages:

```shell
kubectl logs -l app=environmentd -n materialize-environment | grep -i "system.*param"
```

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Materialize CRD Field Descriptions](/installation/appendix-materialize-crd-field-descriptions/)
- [Troubleshooting](/installation/troubleshooting/)
