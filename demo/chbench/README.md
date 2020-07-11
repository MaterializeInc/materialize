# chbench

This is a demonstration of Materialize on [CH-benCHmark]—a mashup of TPC-C and
TPC-H designed to test the speed of analytics queries on a rapidly changing
dataset.

There are several moving pieces to this demo. At the bottom of the stack, we
have a MySQL instance that stores the TPC-C dataset. We connect the
CH-benCHmark's transactional load generator to this MySQL instance, sending a
configurable volume of new orders and such through MySQL. Then, we pipe the
MySQL binlog into Kafka (via Debezium and Kafka Connect, though the details are
not so important), and plug `materialized` into the other end. Then we
install the TPC-H queries into `materialized` as materialized views, and watch
as they magically stay up to date.

The components are orchestrated via [Docker Compose][docker-compose], which
runs each component in a Docker container. You can run this demo via Linux,
an EC2 VM instance, or a Mac laptop. Note that running Docker Compose will cause
some overhead on macOS; to measure performance, you'll want to use Linux.

Should you want to run this demo on a Mac laptop, you'll
want to increase memory available to Docker Engine using the following steps:
   1. Open Docker for Mac's preferences window
   2. Go to the "Advanced" section.
   3. Slide the "Memory" slider to at least 8 GiB.
   4. Click "Apply and Restart".
   5. Continue with the `docker-compose` steps listed above.

## Getting started

Follow the [Metabase demo instructions][demo], which uses this chbench harness.

[demo]: ../../doc/developer/metabase-demo.md

## Using the MySQL CLI

If you want to access a MySQL shell, run the following in the
`demo/chbench` directory:

```
docker-compose run mysqlcli
```

If you've just run `docker-compose up`, you might need to wait a few seconds
before running this.

## Viewing metrics

There are several services that can be used to see how materialize is running. Our custom
system is via grafana, and when you run `docker-compose up` you will get grafana
listening on port 3000.

To view metrics, just visit: http://localhost:3000/d/mz

If you want to be able to edit the dashboard you will need to log in:
http://localhost:3000/login the username/password is admin/admin.

If you don't save the dashboard then **reloading the page will destroy your edits**.
Click the save floppy disk icon and copy the resulting JSON into
`grafana/dashboards/materialize.json`.

## Running with less Docker

Docker can get in the way of debugging materialized—for example, it makes
running `perf` on the materialized binary challenging. There are two easy ways
around this:

  * Running the `materialized` process outside of Docker, as described in
    ["Running with minimal Docker"](docker-local.md).
  * Using the [Nix test harness][nix] in the mtrlz-setup repository.

[nix]: https://github.com/MaterializeInc/mtrlz-setup/tree/master/nix

## Running on AWS EC2

Materialize employees can follow the instructions in the infrastructure
repository for running a semi-automatic load test on AWS EC2.

See: https://github.com/MaterializeInc/infra/tree/main/cloud#starting-a-load-test
