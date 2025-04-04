---
title: "mz-debug emulator"
description: Debug emulator environments running in Docker.
menu:
  main:
    parent: mz-debug
    weight: 20
---

The **emulator** subcommand debugs Docker-based Materialize deployments. It collects:
- Docker logs and resource information
- Snapshots of system catalog tables from your Materialize instance

```console
$ mz-debug emulator [OPTIONS]
```

### Requirements

- Docker installed and running. If [Docker](https://www.docker.com/) is not installed, refer to its
[official documentation](https://docs.docker.com/get-docker/) to install
- A valid Materialize SQL connection URL for your local emulator.

### Flags

| Option                           | Description                                                                                     |
|----------------------------------|-------------------------------------------------------------------------------------------------|
| `--dump-docker`                  | If true, dump debug information from the Docker container. Defaults to `true`.                  |
| `--docker-container-id <ID>`     | The Docker container to dump. Required if `--dump-docker` is true.                             |
| `--mz-connection-url <URL>`      | The URL of the Materialize SQL connection. Defaults to `postgres://127.0.0.1:6875/materialize?sslmode=prefer`. |

### Examples

**Debug a running local emulator container:**
```console
mz-debug emulator \
    --docker-container-id 123abc456def
```

## Files

The debug tool generates files in two main categories: Docker resources and System Catalog. Files are stored under a timestamped directory: `debug-YYYY-MM-DD-HH-TMM-SSZ/` and "zipped" as the directory name.

### Docker Resource Files
Stored under the `docker/<CONTAINER-ID>` sub-directory

| Resource Type | Output File |
|--------------|-------------|
| Container Logs | `logs-stdout.txt` and `logs-stderr.txt` |
| Container Inspection | `inspect.txt` |
| Container Stats | `stats.txt` |
| Container Processes | `top.txt` |

All files are saved in a directory structure: `<base_path>/docker/<container_id>/`.

{{% integrations/mz-debug/global-flags %}}