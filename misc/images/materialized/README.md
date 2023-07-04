This is an all-in-one Docker image of Materialize, a fast, distributed SQL
database built on streaming internals. This image is used for **internal**
development and testing of Materialize.

> **Warning**
>
> You **should not** run production deployments using this Docker image.
>
> This Docker image is **not** supported by Materialize.
>
> This Docker image does **not** support version upgrades.
>
> The performance characteristics of this Docker image are **not** representative
> of the performance characteristics of our hosted offering. This image bundles
> several services into the same container, while in our hosted offering we
> run these services scaled across many machines.

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
