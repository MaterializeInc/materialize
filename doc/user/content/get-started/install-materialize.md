---
title: "Run Materialize on Docker"
description: "Run a containerized version of Materialize for evaluation or testing using Docker."
menu:
  main:
    parent: "get-started"
    weight: 16
    name: "Install Materialize locally"

---

For testing/evaluation purposes, you can download the Materialize Docker image
and run Materilize locally in a Docker container.  The Docker image:

- Includes the [Quickstart](/get-started/quickstart) data source

- Supports connecting your data sources

- Provides [community support via
  Slack](https://materializecommunity.slack.com/join/shared_invite/zt-2bad5ce4i-ZsiPWI5jd7Q9pRDGYj3dkw#/shared-invite/email)

The Docker image is not representative of Materialize's performance and full feature set. To evaluate
Materialize for production scenarios, we recommend signing up for a [free trial
account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation).

{{< warning >}}

The Docker image is for <redb> testing/evaluation purposes only. It is not
suitable for production deployments</redb>.  See also [Limitations](#limitations).

{{</ warning >}}

### License and privacy policy

- Use of the Docker image is subject to Materialize's [BSL
  License](https://github.com/MaterializeInc/materialize/blob/main/LICENSE).

- By downloading the Docker image, you are agreeing to Materialize's [privacy
  policy](https://materialize.com/privacy-policy/).

### Prerequisites

- Docker. If [Docker](https://www.docker.com/) is not installed, refer to its
[official documentation](https://docs.docker.com/get-docker/) to install.

### Run Materialize in a Docker container

{{< note >}}

- Use of the Docker image is subject to Materialize's [BSL
   License](https://github.com/MaterializeInc/materialize/blob/main/LICENSE).

- By downloading the Docker image, you are agreeing to Materialize's [privacy
   policy](https://materialize.com/privacy-policy/).

{{</ note >}}

1. In a terminal, issue the following command to run a Docker container from the
   Materialize image. The command downloads the Materialize image, if one has
   not been already downloaded.

   ```sh
   docker run -v mzdata:/mzdata -p 6875:6875 -p 6876:6876 materialize/materialized
   ```

   When running locally:

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

   For example, if using `psql`:

   ```sh
   psql postgres://materialize@localhost:6875/materialize
   ```

1. Once connected, you can get started with the
   [Quickstart](/get-started/quickstart). Where a command for the Materialize
   console can differ from other SQL clients, both versions of the command are
   presented.

### Next steps

To get started ingesting your own data from an external system like Kafka, MySQL
or PostgreSQL, check the documentation for [sources](/sql/create-source/).

To learn more about Materialize, sign up for a [free
trial](https://materialize.com/register/?utm_campaign=General&utm_source=documentation)
or [schedule a
demo](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).

### Limitations

The Docker image is for <redb> testing/evaluation purposes only</redb>.  That is:

- It is <redb>not</redb> suitable for production deployments.

- Materialize does <redb>not</redb> provide support for the Docker image.

Additionally, the Docker image:

- Does <redb>not</redb> support version upgrades.

- Does <redb>not</redb> provide independent compute/storage scaling

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
