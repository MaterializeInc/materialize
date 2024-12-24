---
title: "Download and run Materialize Emulator"
description: "The Materialize Emulator is an all-in-one Docker image available on Docker Hub, offering the fastest way to get hands-on experience with Materialize in a local environment."
menu:
  main:
    parent: "get-started"
    weight: 16
    name: "Download and run Materialize Emulator"

---

The Materialize Emulator is an all-in-one Docker image available on Docker Hub
for testing and evaluation purposes. The Materialize Emulator is not
representative of Materialize's performance and full feature set. To evaluate
Materialize for production scenarios, sign up for a [free trial
account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation)
or [schedule a demo](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).

{{< warning >}}

The Materialize Emulator is for <redb> testing/evaluation purposes only. It is
not suitable for production deployments</redb>.

{{</ warning >}}

### Materialize Emulator vs Materialize

|                               | Materialize Emulator                                       | Materialize                                                            |
|-------------------------------|----------------------------------------------------|------------------------------------------------------------------------|
|         | <i>The fastest option for prototyping or internal testing with Materialize.</i> | <i>Cloud-hosted SaaS solution for critical operational workloads.</i>|
| **Production deployments**  | ❌ Not suitable due to performance and [license limitations](#license-and-privacy-policy). | ✅ |
| **Performance**             | ❌ Limited. Services are bundled in a single container. | ✅ High. Services are scaled across many machines.|
| **Dedicated support**       | ❌ | ✅ |
| **Sample data**  | ✅ Quickstart data source. | ✅ Quickstart data source. |
| **Data sources**  | ✅ Connect using configuration SQL. | ✅ Connect using a streamlined GUI. |
| **Version upgrades**                 | ✅ Manual, with no data persistence.| ✅ Automated, with data persistence. |
| **Use case isolation**               | ❌ | ✅ |
| **Fault tolerance**                  | ❌ | ✅ |
| **Horizontal scalability**           | ❌ | ✅ |
| **GUI (Materialize Console)**      | ✅ | ✅ |

### Prerequisites

- Docker. If [Docker](https://www.docker.com/) is not installed, refer to its
[official documentation](https://docs.docker.com/get-docker/) to install.

### Run Materialize Emulator

{{< note >}}

- Use of the Docker image is subject to Materialize's [BSL License](https://github.com/MaterializeInc/materialize/blob/main/LICENSE).

- By downloading the Docker image, you are agreeing to Materialize's [privacy policy](https://materialize.com/privacy-policy/).

{{</ note >}}

1. In a terminal, issue the following command to run a Docker container from the
   Materialize Emulator image. The command downloads the image, if one has not
   been already downloaded.

   ```sh
   docker run -d -p 127.0.0.1:6874:6874 -p 127.0.0.1:6875:6875 -p 127.0.0.1:6876:6876 materialize/materialized:{{< version >}}
   ```

   When running locally:

   - The Docker container binds exclusively to localhost for security reasons.
   - The [Materialize Console](/console/) is available on port `6874`.
   - The SQL interface is available on port `6875`.
   - Logs are available via `docker logs <container-id>`.
   - A default user `materialize` is created.
   - A default database `materialize` is created.

1. <a name="materialize-emulator-connect-client"></a>

   Open the Materialize Console in your browser at [http://localhost:6874](http://localhost:6874).

   Alternatively, you can connect to the Materialize Emulator using your
   preferred SQL client, using the following connection field values:

   | Field    | Value         |
   |----------|---------------|
   | Server   | `localhost`   |
   | Database | `materialize` |
   | Port     | `6875`        |
   | Username | `materialize` |

   For example, if using [`psql`](/integrations/sql-clients/#psql):

   ```sh
   psql postgres://materialize@localhost:6875/materialize
   ```

1. Once connected, you can get started with the
   [Quickstart](/get-started/quickstart).

### Next steps

- To start ingesting your own data from an external system like Kafka, MySQL or
  PostgreSQL, check the documentation for [sources](/sql/create-source/).

- Join the [Materialize Community on Slack](https://materialize.com/s/chat).

- To more fully evaluate Materialize and its features, sign up for a [free trial
  account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation)
  or [schedule a demo](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).

### Technical Support

For questions, discussions, or general technical support, join the [Materialize
Community on Slack](https://materialize.com/s/chat).

### License and privacy policy

- Use of the Docker image is subject to Materialize's [BSL
  License](https://github.com/MaterializeInc/materialize/blob/main/LICENSE).

- By downloading the Docker image, you are agreeing to Materialize's
  [privacy policy](https://materialize.com/privacy-policy/).

<style>
red { color: #d33902 }
redb { color: #d33902; font-weight: 500; }
</style>
