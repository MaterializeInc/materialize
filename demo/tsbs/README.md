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
  > /tmp/cpu-10000000
```

Convert the file to CSV:

```shell
</tmp/cpu-10000000 time ./demo/tsbs/influx2csv.py > /tmp/cpu-10000000.csv
```

Read the file in Materialize:

```sql
CREATE SOURCE cpu_1000
    FROM FILE '/tmp/cpu-1000.csv'
    FORMAT CSV WITH HEADER;
```

This works! But it's not interesting so far. We didn't make use of the
timestamps in the data.

Creating a view:

```sql
create MATERIALIZED view cpu_1000_view as select
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
timestamp::int8 / 1000000 as timestamp
 from cpu_1000;
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

```sql
CREATE MATERIALIZED VIEW cpu_1000_transactions AS
SELECT
    *
FROM
    cpu_1000_view
WHERE
    timestamp <= mz_logical_timestamp();
```

```sql
CREATE MATERIALIZED VIEW cpu_1000_transactions2 AS
SELECT
    *
FROM
    cpu_1000_view
WHERE
    timestamp <= mz_logical_timestamp()
    AND timestamp + 2000 >= mz_logical_timestamp();
```

## Reading data through Avro OCF

```shell
./bin/tsbs_generate_data \
  --format influx \
  --scale 10 \
  --timestamp-start \
  2021-06-24T00:00:00Z \
  --timestamp-end 2021-06-25T0:00:36Z \
  --log-interval 10s \
  --use-case cpu-only \
  --seed 123 | (cd ~/dev/repos/materialize/demo/tsbs && pipenv run python influx2avro.py) > /tmp/tsbs.pipe
```

```sql
CREATE MATERIALIZED SOURCE cpu
    FROM AVRO OCF '/tmp/tsbs.pipe'
    FORMAT AVRO USING SCHEMA '/tmp/tsbs.schema'
    WITH (tail = true)
    ENVELOPE MATERIALIZE;

```

## Reading data through Avro/Kafka!

```shell
kafka-topics --topic tsbs --delete --bootstrap-server localhost:9092
curl -X DELETE http://localhost:8081/subjects/tsbs-value
kafka-topics --topic tsbs --create --bootstrap-server localhost:9092
./bin/tsbs_generate_data \
  --format influx \
  --scale 1000 \
  --timestamp-start \
  2021-06-24T00:00:00Z \
  --timestamp-end 2021-06-25T0:00:36Z \
  --log-interval 1s \
  --use-case cpu-only \
  --seed 123 | (cd ~/dev/repos/materialize/demo/tsbs && pipenv run python influx2avro_append.py) \
  | LOG_DIR=/tmp kafka-avro-console-producer --topic tsbs --bootstrap-server localhost:9092 --property value.schema.file=/tmp/tsbs.schema
```

```sql
DROP VIEW IF EXISTS tsbs_avg;
DROP SOURCE IF EXISTS tsbs;
CREATE MATERIALIZED SOURCE tsbs
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'tsbs'
  FORMAT AVRO USING SCHEMA FILE '/tmp/tsbs.schema'
  ENVELOPE MATERIALIZE;
CREATE OR REPLACE MATERIALIZED VIEW tsbs_avg AS (
    SELECT
        hostname,
        avg(usage_user),
        avg(usage_system),
        avg(usage_idle),
        avg(usage_nice),
        avg(usage_iowait),
        avg(usage_irq),
        avg(usage_softirq),
        avg(usage_steal),
        avg(usage_guest),
        avg(usage_guest_nice)
    FROM tsbs
    WHERE mz_logical_timestamp() >= time
      AND mz_logical_timestamp()  < time + 3600000
    GROUP BY hostname
);
COPY (TAIL tsbs_avg) TO STDOUT;
```


```sql
DROP VIEW IF EXISTS tsbs_avg;
DROP VIEW IF EXISTS tsbs_1h;
DROP SOURCE IF EXISTS tsbs;
CREATE SOURCE tsbs
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'tsbs'
  FORMAT AVRO USING SCHEMA FILE '/tmp/tsbs.schema'
  ENVELOPE MATERIALIZE;
CREATE OR REPLACE MATERIALIZED VIEW tsbs_1h AS (
    SELECT *
    FROM tsbs
    WHERE mz_logical_timestamp() >= time
      AND mz_logical_timestamp()  < time + 3600000
);
CREATE OR REPLACE MATERIALIZED VIEW tsbs_avg AS (
    SELECT
        hostname,
        avg(usage_user),
        avg(usage_system),
        avg(usage_idle),
        avg(usage_nice),
        avg(usage_iowait),
        avg(usage_irq),
        avg(usage_softirq),
        avg(usage_steal),
        avg(usage_guest),
        avg(usage_guest_nice)
    FROM tsbs_1h
    GROUP BY hostname
);
COPY (TAIL tsbs_avg) TO STDOUT;
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
