# mz-debug emulator

Use mz-debug to debug Materialize Emulator environments running in Docker.



`mz-debug emulator` debugs Docker-based Materialize deployments. It collects:

- Docker logs and resource information.
- Snapshots of system catalog tables from your Materialize instance.

## Requirements

- Docker installed and running. If [Docker](https://www.docker.com/) is not installed, refer to its
[official documentation](https://docs.docker.com/get-docker/) to install
- A valid Materialize SQL connection URL for your local emulator.

## Syntax

```shell
mz-debug emulator [OPTIONS]
```

## Options

### `mz-debug emulator` options


| Option | Description |
| --- | --- |
| <code>--docker-container-id &lt;ID&gt;</code> | <p><a name="docker-container-id"></a> The Docker container to dump.</p> <p>Required.</p>  |
| <code>--dump-docker &lt;boolean&gt;</code> | <p><a name="dump-docker"></a> If <code>true</code>, dump debug information from the Docker container.</p> <p>Defaults to <code>true</code>.</p>  |


### `mz-debug` global options


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

The generated debug files are in two main categories: [Docker resource
files](#docker-resource-files) and [system catalog
files](#system-catalog-files).

### Docker resource files

In `mz_debug_YYYY-MM-DD-HH-TMM-SSZ/`, under the `docker/<CONTAINER-ID>`
sub-directory,  the following Docker resource debug files are generated:


| Resource Type | Files |
| --- | --- |
| Container Logs | <ul> <li><code>logs-stdout.txt</code></li> <li><code>logs-stderr.txt</code></li> </ul>  |
| Container Inspection | <ul> <li><code>inspect.txt</code></li> </ul>  |
| Container Stats | <ul> <li><code>stats.txt</code></li> </ul>  |
| Container Processes | <ul> <li><code>top.txt</code></li> </ul>  |


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


## Example

### Debug a running local emulator container
```console
mz-debug emulator \
    --docker-container-id 123abc456def
```
