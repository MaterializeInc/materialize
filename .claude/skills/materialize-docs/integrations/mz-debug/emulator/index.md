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

{{< yaml-table data="mz-debug/emulator_options" >}}

### `mz-debug` global options

{{< yaml-table data="mz-debug/mz_debug_option" >}}

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

{{< yaml-table data="mz-debug/docker_resource_files" >}}

{{% integrations/mz-debug/system-catalog-files %}}

{{% integrations/mz-debug/prometheus-files %}}

{{% integrations/mz-debug/memory-profiles %}}

## Example

### Debug a running local emulator container
```console
mz-debug emulator \
    --docker-container-id 123abc456def
```

