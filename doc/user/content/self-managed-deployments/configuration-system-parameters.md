---
title: "Configuring System Parameters"
description: "How to configure system parameters for Materialize using a Kubernetes ConfigMap"
aliases:
  - /self-managed/configuration-system-parameters/
menu:
  main:
    parent: "sm-deployments"
    name: "Configure system parameters"
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

For balancerd settings, such as its connection limit, see
[Configure balancerd dynamic configuration](#configure-balancerd-dynamic-configuration).

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

Kubernetes periodically refreshes mounted ConfigMaps. The delay depends on
the kubelet sync period and its ConfigMap cache. With a one-minute sync period
and a one-minute cache lifetime, propagation can take up to two minutes.
See [Kubernetes ConfigMap update behavior](https://kubernetes.io/docs/tasks/configure-pod-container/configure-pod-configmap/#mounted-configmaps-are-updated-automatically).

Once the ConfigMap is synced to the volume, Materialize checks for configuration
changes every second and applies them automatically.

To request an earlier refresh, update an annotation on each affected pod,
replacing `<pod-name>` with the pod's name:

```shell
kubectl annotate pod <pod-name> \
  -n materialize-environment \
  configmap-reload-trigger="$(date +%s)" \
  --overwrite
```

{{< note >}}

Even after the ConfigMap is synced, some system parameters may require a restart to
take effect.

{{< /note >}}

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
| `statement_logging_max_sample_rate` | Cap on the fraction of statements recorded in [query history](/self-managed-deployments/query-history/). Setting it here overrides the Helm chart value. |
| `statement_logging_target_data_rate` | Sustained bytes per second that statement logging may write. Bounds query history growth on busy instances. |

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

## Configure balancerd dynamic configuration

{{< warn-if-unreleased "v26.44" >}}

To configure balancerd, use a separate ConfigMap referenced by
`spec.balancerdConfigmapName`. This example requires Materialize Operator and a
Materialize instance running v26.44 or later, with balancerd enabled. The field is
supported in both the `v1` and `v1alpha1` Materialize custom resources.

Balancerd configuration is separate from environmentd system parameters.
For example, `balancerd_max_connections` limits connections per balancerd process,
while `max_connections` controls connections in environmentd. Do not put
`balancerd_*` settings in `system-params.json` or set them with `ALTER SYSTEM SET`.

### Create the balancerd ConfigMap

Save the following as `balancerd-configmap.yaml`, using the same namespace as your
Materialize instance:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mz-balancerd-config
  namespace: materialize-environment
data:
  config.json: |
    {
      "balancerd_max_connections": 10000
    }
```

The `config.json` key must contain a valid JSON object. Use `{}` if you do not
need any overrides yet. Apply the ConfigMap before referencing it:

```shell
kubectl apply -f balancerd-configmap.yaml
```

You manage this ConfigMap, either directly or through your deployment tooling.
The operator mounts it without creating, overwriting, or deleting it.

### Reference the ConfigMap

Set `spec.balancerdConfigmapName` in your Materialize manifest and apply it.
For an existing instance, you can also patch the resource. Replace
`<instance-name>` with the name of your Materialize resource:

```shell
kubectl patch materialize <instance-name> -n materialize-environment \
  --type merge \
  -p '{"spec":{"balancerdConfigmapName":"mz-balancerd-config"}}'
```

Adding, changing, or removing this reference rolls the balancerd pods but does
not require an environmentd rollout or a change to `requestRollout`. If the
ConfigMap or its `config.json` key is missing, the new pods cannot start.

For a standalone `Balancer` custom resource, set `spec.configmapName` instead.

### Verify the configured connection limit

Find the balancerd pods for your instance, replacing `<instance-name>` with your
Materialize resource's name:

```shell
kubectl get pods -n materialize-environment \
  -l 'app=balancerd,materialize.cloud/organization-name=<instance-name>'
```

Forward a pod's internal HTTP port. Replace `<balancerd-pod-name>` with one of
those pod names. The default internal HTTP port is `8080`:

```shell
kubectl port-forward -n materialize-environment pod/<balancerd-pod-name> 8080:8080
```

In another terminal, check the configured limit:

```shell
curl -s http://localhost:8080/metrics | grep '^mz_balancer_connection_limit '
```

For this example, the result is:

```nofmt
mz_balancer_connection_limit 10000
```

Repeat for each balancerd pod. This metric reports the configured limit, not the
number of active connections.

### Update balancerd configuration

Edit `config.json` in `balancerd-configmap.yaml` and reapply the file:

```shell
kubectl apply -f balancerd-configmap.yaml
```

Changing the ConfigMap contents does not restart balancerd. After Kubernetes
projects the update into the pod, balancerd reads it on its next one-second sync
tick. Allow for the additional [ConfigMap propagation delay](#configmap-sync-behavior)
before verifying the updated metric.

Keep these behaviors in mind:

- Removing a setting from the JSON object does not reset its running value. To
  reset a setting at runtime, explicitly set its default value.
- Invalid JSON at startup prevents the file sync loop from starting. Correct the
  ConfigMap and restart the affected balancerd pods. Invalid JSON introduced
  after a successful startup leaves the previous values in use, and syncing
  resumes after the JSON is corrected.
- `balancerd_max_connections` defaults to `5000` per process and covers pgwire
  and HTTPS connections together. Setting it to `0` disables this limit. The
  separate environmentd `max_connections` limit still applies.

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

- [Query History](/self-managed-deployments/query-history/)
- [Materialize Operator Configuration](/installation/configuration/)
- [Materialize CRD Field Descriptions](/installation/appendix-materialize-crd-field-descriptions/)
- [Troubleshooting](/installation/troubleshooting/)
