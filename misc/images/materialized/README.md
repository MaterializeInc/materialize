# `materialized` Docker image

This directory builds an all-in-one Docker image of Materialize for our internal
development and testing.

> ⚠️ **WARNING** ⚠️
>
> This Docker image is not officially supported by Materialize. We do not
> offer support for this Docker image. Do not run production deployments using
> this Docker image.

> ⚠️ **WARNING** ⚠️
>
> The performance characteristics of this Docker image are not representative
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
