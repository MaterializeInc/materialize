The Materialize Emulator is an all-in-one Docker image available on Docker Hub, offering the fastest way to get hands-on experience with Materialize in a local environment.

|                           | **Materialize Emulator**                                          | **Materialize**                                                              |
|---------------------------|-------------------------------------------------------------------|------------------------------------------------------------------------------|
| **Description**           | The fastest option for prototyping or internal testing with Materialize.              | Materialize's cloud hosted SaaS solution for critical operational workloads. [Try it free](https://materialize.com/try/) or [book a demo](https://materialize.com/demo/)            |
| **Production deployments** | ❌ Not suitable due to performance and license limitations.        | ✔️ Supported.                                                                 |
| **Performance**     | ❌ Limited. Several bundled services in a single container                         | ✔️ High. Services are scaled across many machines.                              |
| **Support**                | ❌ Limited support (via [Slack Community](https://materialize.com/s/chat))  | ✔️ Dedicated technical support                                                |
| **Sample data**            | ✔️ Quickstart data source                                                    | ✔️ Quickstart data source                                                    |
| **Data sources**           | ✔️ Manual configuration using SQL only                                       | ✔️ Streamlined GUI-based setup                                       |
| **Version upgrades**       | ❌ Manual, with no data persistence                                         | ✔️ Automated                                                                  |
| **Use case isolation**     | ❌                                                                          | ✔️                                                                           |
| **Fault tolerance**        | ❌                                                                          | ✔️                                                                           |
| **Horizontal scalability** | ❌                                                                          | ✔️                                                                           |
| **GUI**                    | ❌                                                                          | ✔️ Materialize Console (dedicated web app).       |

## License and Privacy Policy

* Use of the Docker image is subject to Materialize’s [BSL License](https://github.com/MaterializeInc/materialize/blob/main/LICENSE).
* By downloading the Docker image, you are agreeing to Materialize’s [privacy policy](https://materialize.com/privacy-policy/).


## Usage

To launch the Docker container:

```
docker run -v mzdata:/mzdata -p 6875:6875 -p 6876:6876 materialize/materialized
```

After running this command...

  * Materialize logs will be emitted to the stderr stream.
  * The SQL interface will be available on port 6875.
  * The HTTP interface will be available on port 6876.

To connect to the SQL interface using `psql`:

```
psql postgres://materialize@localhost:6875/materialize
```

To view logs for the embedded CockroachDB server:

```
docker exec <CONTAINER-ID> cat /mzdata/cockroach/logs/cockroach.log
```
