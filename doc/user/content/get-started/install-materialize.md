---
title: "Install Materialize locally"
description: "Download the Docker image and run Materialize locally for evaluation"
menu:
  main:
    parent: "get-started"
    weight: 16
    name: "Install Materialize locally"

---

For testing/evaluation purposes, you can download the Materialize Docker image
and run Materilize locally in a Docker container. To more fully evaluate
Materialize and its features, Materialize offers a [free trial
account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation).


{{< warning >}}

The Docker image is for <redb> testing/evaluation purposes only. It is not
suitable for production deployments</redb>.  See also [Limitations](#limitations).

{{</ warning >}}

### Prerequisites

- Docker. If [Docker](https://www.docker.com/) is not installed, refer to its
[official documentation](https://docs.docker.com/get-docker/) to install.

### Run Materialize in a Docker container

1. In a terminal, issue the following command to run a Docker container from the
   Materialize image. The command downloads the Materialize image, if one has
   not been already downloaded.

   ```sh
   docker run -v mzdata:/mzdata -p 6875:6875 -p 6876:6876 materialize/sh-materialized:v1.0.0
   ```

   - Materialize logs are emitted to the `stderr`.
   - The SQL interface is available on port `6875`.
   - The HTTP interface is available on port `6876`.
   - A default user `materialize` is created.
   - A default database `materialize` is created.

1. Connect to Materialize using your preferred SQL client, using the following
   connection field values:

   | Field    | Value         |
   |----------|---------------|
   | Server   | `localhost`   |
   | Database | `materialize` |
   | Port     | `6875`        |
   | Username | `materialize` |

   For example, if using `psql`

   ```sh
   psql postgres://materialize@localhost:6875/materialize
   ```

1. Once connected, you can get started with the
   [Quickstart](/get-started/quickstart). Where a command for the Materialize
   console can differ from other SQL clients, both versions of the command are
   presented.

### Check logs

To view logs for the embedded CockroachDB server, run the following steps in a
terminal window.

1. Find your Docker container id:

   ```sh
   docker ps
   ```

2. Run the following command, substituting `<CONTAINER-ID>` wit your container
   id:

   ```sh
   docker exec <CONTAINER-ID> cat /mzdata/cockroach/logs/cockroach.log
   ```

### Next steps

Sign up for a [free
trial](https://materialize.com/register/?utm_campaign=General&utm_source=documentation)
or [schedule a demo](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation) to learn more about Materialize.

### Limitations

{{< warning >}}

The Docker image is for <redb> testing/evaluation purposes only</redb>.  That is:

- It is <redb>not</redb> suitable for production deployments.

- Materialize does <redb>not</redb> provide support for the Docker image.

{{</ warning >}}

Some limitations of the Docker image include:

- Does <redb>not</redb> support version upgrades.

- Does <redb>not</redb> provide independent scaling of storage and compute.

- Does <redb>not</redb> provide use case isolation.

- Does <redb>not</redb> provide fault tolerance.

- Does <redb>not</redb> provide horizontal scalability.

- Does <redb>not</redb> include the Console.

- Does <redb>not</redb> provide the performance of Materialize's hosted
  offering. That is, the performance characteristics of the Docker image are
  <redb>not</redb> representative of the performance characteristics of
  Materialize's hosted offering. The image bundles several services into the
  same container while in the hosted offering, the services are scaled across
  many machines.

{{< tip >}}

To more fully evaluate Materialize and its features, Materialize offers a [free
trial
account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation).

{{</ tip >}}

<style>
red { color: #d33902 }
redb { color: #d33902; font-weight: 500; }
</style>
