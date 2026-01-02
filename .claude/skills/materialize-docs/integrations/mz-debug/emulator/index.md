---
audience: developer
canonical_url: https://materialize.com/docs/integrations/mz-debug/emulator/
complexity: intermediate
description: Use mz-debug to debug Materialize Emulator environments running in Docker.
doc_type: reference
keywords:
- mz-debug emulator
product_area: General
status: stable
title: mz-debug emulator
---

# mz-debug emulator

## Purpose
Use mz-debug to debug Materialize Emulator environments running in Docker.

If you need to understand the syntax and options for this command, you're in the right place.


Use mz-debug to debug Materialize Emulator environments running in Docker.


`mz-debug emulator` debugs Docker-based Materialize deployments. It collects:

- Docker logs and resource information.
- Snapshots of system catalog tables from your Materialize instance.

## Requirements

- Docker installed and running. If [Docker](https://www.docker.com/) is not installed, refer to its
[official documentation](https://docs.docker.com/get-docker/) to install
- A valid Materialize SQL connection URL for your local emulator.

## Syntax

This section covers syntax.

```shell
mz-debug emulator [OPTIONS]
```bash

## Options

This section covers options.

### `mz-debug emulator` options

<!-- Dynamic table: mz-debug/emulator_options - see original docs -->

### `mz-debug` global options

<!-- Dynamic table: mz-debug/mz_debug_option - see original docs -->

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

<!-- Dynamic table: mz-debug/docker_resource_files - see original docs -->

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: integrations/mz-debug/system-catalog-fil --> --> -->

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: integrations/mz-debug/prometheus-files --> --> -->

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: integrations/mz-debug/memory-profiles --> --> -->

## Example

This section covers example.

### Debug a running local emulator container
```console
mz-debug emulator \
    --docker-container-id 123abc456def
```