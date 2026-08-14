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
parameter for a single cluster or replica instead, see [Scoping Parameters to a
Cluster or Replica](#scoping-parameters-to-a-cluster-or-replica).

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

## Scoping Parameters to a Cluster or Replica

Every top-level key in `system-params.json` sets a parameter for the whole
environment, with two exceptions: `clusters` and `replicas` are reserved keys
that hold per-cluster and per-replica overrides.

- `clusters` is keyed by cluster name.
- `replicas` is keyed by cluster name, then by replica name. The nesting is
  required because a replica name is only unique within its cluster, and because
  both names may themselves contain a `.`.

For example, the following configuration sets environment-wide parameters and
overrides parameters for a specific cluster and replica:

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
      "clusters": {
        "analytics": {
          "enable_eager_delta_joins": true
        }
      },
      "replicas": {
        "analytics": {
          "r1": {
            "enable_lgalloc": false
          }
        }
      }
    }
```

In this example, `max_connections` and `enable_lgalloc` apply environment-wide,
the `analytics` cluster additionally enables `enable_eager_delta_joins`, and the
`r1` replica of `analytics` turns `enable_lgalloc` back off for itself.

A ConfigMap that uses neither reserved key is a plain environment-wide parameter
map. No system parameter is named `clusters` or `replicas`, so a flat ConfigMap
can never be reinterpreted as a scoped one.

Behavior worth knowing:

- **Not every parameter can be scoped.** Only parameters whose scope is
  `cluster` or `replica` are resolved per object. An entry for any other
  parameter is ignored, so set it as a top-level key instead.
- **Unknown names are ignored, not rejected.** A section naming a cluster or
  replica that does not exist has no effect and logs no error. If you later
  create an object with that name, the override applies to it.
- **A value that matches the environment-wide value records no override.**
  Overrides are only stored where they actually differ.
- **An unparseable value is dropped, not rejected.** The rest of the file still
  applies and `environmentd` logs a warning naming the parameter and the object.
- **Removing a name or an entry removes the override**, returning the object to
  the environment-wide value.

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
