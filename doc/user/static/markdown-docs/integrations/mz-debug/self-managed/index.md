# mz-debug self-managed
Use mz-debug to debug Self-Managed Materialize Kubernetes environments.
`mz-debug self-managed` debugs Kubernetes-based Materialize deployments. It
collects:

- Logs and resource information from pods, daemonsets, and other Kubernetes
    resources.

- Snapshots of system catalog tables from your Materialize instance.

By default, the tool will automatically port-forward to collect system catalog information. You can disable this by specifying your own connection URL via `--mz-connection-url`.

## Requirements

`mz-debug` requires [`kubectl`](https://kubernetes.io/docs/tasks/tools/)
v1.32.3+. Install [kubectl](https://kubernetes.io/docs/tasks/tools/) if you do
not have it installed.

## Syntax

```console
mz-debug self-managed [OPTIONS]
```

## Options

## `mz-debug self-managed` options


| Option | Description |
| --- | --- |
| <code>--k8s-namespace &lt;NAMESPACE&gt;</code> | <a name="k8s-namespace"></a> <strong>Required</strong>. The Kubernetes namespace of the Materialize instance. |
| <code>--mz-instance-name &lt;MZ_INSTANCE_NAME&gt;</code> | <a name="mz-instance-name"></a> <strong>Required</strong>. The Materialize instance to target. |
| <code>--dump-k8s &lt;boolean&gt;</code> | <p><a name="dump-k8s"></a> If <code>true</code>, dump debug information from the Kubernetes cluster.</p> <p>Defaults to <code>true</code>.</p>  |
| <code>--additional-k8s-namespace &lt;NAMESPACE&gt;</code> | <a name="additional-k8s-namespace"></a> Additional k8s namespaces to dump. |
| <code>--k8s-context &lt;CONTEXT&gt;</code> | <p><a name="k8s-context"></a> The Kubernetes context to use.</p> <p>Defaults to the <code>KUBERNETES_CONTEXT</code> environment variable.</p>  |
| <code>-h</code>, <code>--help</code> | <a name="help"></a> Print help information. |


## `mz-debug` global options


| Option | Description |
| --- | --- |
| <code>--dump-heap-profiles &lt;boolean&gt;</code> | <p><a name="dump-heap-profiles"></a> If <code>true</code>, dump heap profiles (.pprof.gz) from your Materialize instance.</p> <p>Defaults to <code>true</code>.</p>  |
| <code>--dump-prometheus-metrics &lt;boolean&gt;</code> | <p><a name="dump-prometheus-metrics"></a> If <code>true</code>, dump prometheus metrics from your Materialize instance.</p> <p>Defaults to <code>true</code>.</p>  |
| <code>--dump-system-catalog &lt;boolean&gt;</code> | <p><a name="dump-system-catalog"></a> If <code>true</code>, dump the system catalog from your Materialize instance.</p> <p>Defaults to <code>true</code>.</p>  |
| <code>--mz-username &lt;USERNAME&gt;</code> | <p><a name="mz-username"></a> The username to use to connect to Materialize.</p> <p>Can also be set via the <code>MZ_USERNAME</code> environment variable.</p>  |
| <code>--mz-password &lt;PASSWORD&gt;</code> | <p><a name="mz-password"></a> The password to use to connect to Materialize if password authentication is enabled.</p> <p>Can also be set via the <code>MZ_PASSWORD</code> environment variable.</p>  |
| <code>--mz-connection-url &lt;URL&gt;</code> | <p><a name="mz-connection-url"></a>The Materialize instance&rsquo;s <a href="https://www.postgresql.org/docs/14/libpq-connect.html#LIBPQ-CONNSTRING" >PostgreSQL connection URL</a>.</p> <p>Defaults to <code>postgres://127.0.0.1:6875/materialize?sslmode=prefer</code>.</p>  |


## Output

The `mz-debug` outputs its log file (`tracing.log`) and the generated debug
files into a directory named `mz_debug_YYYY-MM-DD-HH-TMM-SSZ/` as well as zips
the directory and its contents `mz_debug_YYYY-MM-DD-HH-TMM-SSZ.zip`.

The generated debug files are in two main categories: [Kubernetes resource
files](#kubernetes-resource-files) and [system catalog
files](#system-catalog-files).

### Kubernetes resource files

Under `mz_debug_YYYY-MM-DD-HH-TMM-SSZ/`, the following Kubernetes resource debug
files are generated:


| Resource Type | Files |
| --- | --- |
| Workloads | <ul> <li><code>pods/{namespace}/*.yaml</code></li> <li><code>logs/{namespace}/{pod}.current.log</code></li> <li><code>logs/{namespace}/{pod}.previous.log</code></li> <li><code>deployments/{namespace}/*.yaml</code></li> <li><code>statefulsets/{namespace}/*.yaml</code></li> <li><code>replicasets/{namespace}/*.yaml</code></li> <li><code>events/{namespace}/*.yaml</code></li> <li><code>materializes/{namespace}/*.yaml</code></li> </ul>  |
| Networking | <ul> <li><code>services/{namespace}/*.yaml</code></li> <li><code>networkpolicies/{namespace}/*.yaml</code></li> <li><code>certificates/{namespace}/*.yaml</code></li> </ul>  |
| Storage | <ul> <li><code>persistentvolumes/*.yaml</code></li> <li><code>persistentvolumeclaims/{namespace}/*.yaml</code></li> <li><code>storageclasses/*.yaml</code></li> </ul>  |
| Configuration | <ul> <li><code>roles/{namespace}/*.yaml</code></li> <li><code>rolebinding/{namespace}/*.yaml</code></li> <li><code>configmaps/{namespace}/*.yaml</code></li> <li><code>serviceaccounts/{namespace}/*.yaml</code></li> </ul>  |
| Cluster-level | <ul> <li><code>nodes/*.yaml</code></li> <li><code>daemonsets/*.yaml</code></li> <li><code>mutatingwebhookconfigurations/{namespace}/*.yaml</code></li> <li><code>validatingwebhookconfigurations/{namespace}/*.yaml</code></li> <li><code>customresourcedefinitions/*.yaml</code></li> </ul>  |


Each resource type directory also contains a `describe.txt` file with the output of `kubectl describe` for that resource type.

### System catalog files

`mz-debug` outputs system catalog files if
[`--dump-system-catalog`](#dump-system-catalog) is `true` (the default).

The generated files are in `system-catalog` sub-directory as `*.csv` files and
contains:

- Core catalog object definitions
- Cluster and compute-related information
- Data freshness and frontier metric
- Source and sink metrics
- Per-replica introspection metrics (under `{cluster_name}/{replica_name}/*.csv`)

For more information about each relation, view the [system
catalog](/sql/system-catalog/).


### Prometheus metrics

`mz-debug` outputs snapshots of prometheus metrics per service (i.e. environmentd) if
[`--dump-prometheus-metrics`](#dump-prometheus-metrics) is `true` (the default).
Each file is stored under `prom_metrics/{service}.txt`.


### Memory profiles

By default, `mz-debug` outputs heap profiles for each service in `profiles/{service}.memprof.pprof.gz`. To turn off this behavior, you can set [`--dump-heap-profiles`](#dump-heap-profiles) to false.




## Prerequisite: Get the Materialize instance name

To use `mz-debug`, you need to specify the <a href="#k8s-namespace">Kubernetes namespace (`--k8s-namespace`)</a> and the <a href="#mz-instance-name">Materialize instance name (`--mz-instance-name`)</a>. To retrieve the Materialize instance name, you can use kubectl. For example, the following retrieves the name of the Materialize instance(s) running in the Kubernetes namespace `materialize-environment`:
```
kubectl --namespace materialize-environment get materializes.materialize.cloud
```
The command should return the NAME of the Materialize instance(s) in the namespace:
```
NAME
12345678-1234-1234-1234-123456789012
```

## Examples

### Debug a Materialize instance running in a namespace

The following example uses `mz-debug` to collect debug information for the Materialize instance (`12345678-1234-1234-1234-123456789012` obtained in the Prerequisite) running in the Kubernetes namespace `materialize-environment`:

```shell
mz-debug self-managed --k8s-namespace materialize-environment \
--mz-instance-name 12345678-1234-1234-1234-123456789012
```

### Include information from additional kubernetes namespaces

When debugging a Materialize instance, you can also include information from other namespaces via <a href="#additional-k8s-namespace">`--additional-k8s-namespace`</a>. The following example collects debug information for the Materialize instance running in the Kubernetes namespace `materialize-environment` as well as debug information for the namespace `materialize`:

```shell
mz-debug self-managed --k8s-namespace materialize-environment \
--mz-instance-name 12345678-1234-1234-1234-123456789012 \
--additional-k8s-namespace materialize
```
