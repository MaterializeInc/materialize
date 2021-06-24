# TSBS on Materialize

An attempt to
replicate https://questdb.io/blog/2021/06/16/high-cardinality-time-series-data-performance/
on Materialize.

## Reading from file

Generate the test data. This will generate a ~10GiB file.

```shell
./bin/tsbs_generate_data \
  --format influx \
  --scale 10000000 \
  --timestamp-start \
  2016-01-01T00:00:00Z \
  --timestamp-end 2016-01-01T0:00:36Z \
  --log-interval 10s \
  --use-case cpu-only \
  --seed 123 \
  > /tmp/cpu-10000000=
```

Convert the file to CSV:

```shell
</tmp/cpu-10000000 time ./demo/tsbs/influx2csv.py > /tmp/cpu-10000000.csv
```

Read the file in Materialize:

```sql
CREATE SOURCE cpu_10000000
    FROM FILE '/tmp/cpu-10000000.csv'
    FORMAT CSV WITH HEADER;
```

This works! But it's not interesting so far. We didn't make use of the
timestamps in the data.

Creating a view:

```sql
create view cpu_10000000_view as select
cpu,
hostname,
region,
datacenter,
rack,
os,
arch,
team,
service,
service_version,
service_environment,
usage_user::int,
usage_system::int,
usage_idle::int,
usage_nice::int,
usage_iowait::int,
usage_irq::int,
usage_softirq::int,
usage_steal::int,
usage_guest::int,
usage_guest_nice::int,
timestamp::long
 from cpu_10000000;
```

Creating an index. The documentation for creating an index states it converts a
view to a materialized view. But it's sparse on what that means and under what
situations the index can reveal its contents:

```sql
create index cpu_10000000_index on cpu_10000000_view (
cpu,
hostname,
region,
datacenter,
rack,
os,
arch,
team,
service,
service_version,
service_environment
);
```

## Reading through Postgres

Follow https://materialize.com/docs/sql/create-source/postgres/ to enable the
Postgres integration.

Set `MZ_HOME` to point to the Materialize source tree.

Loading the data into Postgres, issue the following command from TSBS's
directory:

```shell
./bin/tsbs_load load timescaledb --config $MZ_HOME/demo/tsbs/config.yaml
```

Note: For the initial loading set `do-create-db` in `config.yaml` to `true`. On
subsequent invocations, set it to `false` again.

Typical workflow:

```shell
psql < $MZ_HOME/demo/postgres-setup.sql
psql -p 6875 -h 127.0.0.1  -U materialize < $MZ_HOME/demo/tsbs/mz.sql
./bin/tsbs_load load timescaledb --config $MZ_HOME/demo/tsbs/config.yaml
```

Cleanup to not break the publication protocol:

```shell
psql < $MZ_HOME/demo/postgres-teardown.sql
```
