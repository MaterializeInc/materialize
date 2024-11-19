The Materialize Emulator is an all-in-one Docker image available on Docker Hub, offering the fastest way to get hands-on
experience with Materialize in a local environment.

|                           | **Materialize Emulator**                                          | **Materialize**                                                              |
|---------------------------|-------------------------------------------------------------------|------------------------------------------------------------------------------|
| **Description**           | The fastest option for prototyping or internal testing with Materialize.     | Materialize's cloud hosted SaaS solution for critical operational workloads. [Try it free](https://materialize.com/register/) or [book a demo](https://materialize.com/demo/)            |
| **Production deployments** | ❌ Not suitable due to performance and license limitations.                 | ✔️
| **Performance**            | ❌ Limited. Services are bundled in a single container.                     | ✔️ High. Services are scaled across many machines.                           |
| **Dedicated Support**      | ❌                                                                          | ✔️                                                                           |
| **Sample data**            | ✔️ Quickstart data source                                                   | ✔️ Quickstart data source                                                    |
| **Data sources**           | ✔️ Connect using SQL configuration                                          | ✔️ Connect using a streamlined GUI                                           |
| **Version upgrades**       | ✔️ Manual, with no data persistence                                         | ✔️ Automated, with data persistence                                          |
| **Use case isolation**     | ❌                                                                          | ✔️                                                                           |
| **Fault tolerance**        | ❌                                                                          | ✔️                                                                           |
| **Horizontal scalability** | ❌                                                                          | ✔️                                                                           |
| **GUI**                    | ❌                                                                          | ✔️ Materialize Console                                                       |

## Resource requirements

The Materialize Emulator requires resources proportional to the workload size and complexity. Each workload comes
at different resource requirements, but as a starting point we recommend the following allocation, which needs
to be adjusted based on replica sizes.
* At least 16GiB of RAM.
* At least 4 vCPU cores.
* At least 50GiB of disk space.

The emulator can run with less resources, but performance may be degraded. While Materialize Cloud
is designed to scale horizontally, the emulator is limited to a single node without performance isolation.
This means a single replica can consume all available resources, potentially impacting other replicas.

The emulator uses disk space for storing data, logs, and metadata. The emulator requires an NVMe SSD
for optimal performance.

The emulator's performance is not representative of a real Materialize deployment.

## Cluster and replica sizes

The emulator supports the similar cluster and replica sizes that Materialize offers. The sizes represent a
proportional increase in CPU resources. For more details, consult the documentation on [cluster sizes](https://materialize.com/docs/sql/create-cluster/#size).

## Usage

To launch the Docker container:

```
docker pull materialize/materialized:latest
docker run -d -p 127.0.0.1:6875:6875 -p 127.0.0.1:6876:6876 materialize/materialized:latest
```

After running this command...

  * The SQL interface will be available on port 6875.
  * Logs are available via `docker logs <container-id>`.

To connect to the SQL interface using `psql`:

```
psql postgres://materialize@localhost:6875/materialize
```

To view logs for the embedded PostgreSQL server:

```
docker exec <CONTAINER-ID> cat /var/log/postgresql/postgresql-16-main.log
```

## Technical Support
For questions, discussions, or general technical support, join the [Materialize Community on Slack](https://materialize.com/s/chat).

## License and Privacy Policy

* Use of the Docker image is subject to Materialize’s [BSL License](https://github.com/MaterializeInc/materialize/blob/main/LICENSE).
* By downloading the Docker image, you are agreeing to Materialize’s [privacy policy](https://materialize.com/privacy-policy/).
