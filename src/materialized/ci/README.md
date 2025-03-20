The Materialize Emulator is an all-in-one Docker image available on Docker Hub, offering the fastest way to get hands-on experience with Materialize in a local environment.

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

To view logs for the embedded CockroachDB server:

```
docker exec <CONTAINER-ID> cat /mzdata/cockroach/logs/cockroach.log
```

## Technical Support
For questions, discussions, or general technical support, join the [Materialize Community on Slack](https://materialize.com/s/chat).

## License and Privacy Policy

* Use of the Docker image is subject to Materialize’s [BSL License](https://github.com/MaterializeInc/materialize/blob/main/LICENSE).
* By downloading the Docker image, you are agreeing to Materialize’s [privacy policy](https://materialize.com/privacy-policy/).
