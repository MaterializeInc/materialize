# Introduction

The canary environment workflow creates various database objects against the Materialize Production Sandbox

The goals of this framework are:
- create long-lived database objects in Materialize Production Sandbox whose operation will then be monitored
  periodically going forward.

- dog-food the use of dbt against Materialize

Objects that are supported by dbt are created using dbt. Objects that are outside of the scope of dbt
are created using testdrive.

# Workload

## TPC-H Generator (`models/tpc-h`)

A set of two materialized views based on TPC-H queries are defined to run against a `GENERATOR TPCH` source.

The TPC-H queries to materialize were chosen as to include both outer joins and aggregations.

## Postgres source (`models/pg-cdc`)

The postgres source is an RDS instance running in AWS that is replicating two tables.

In order to continously emit updates, pg_cron jobs that update the tables are set up to run with a 1-second interval.

## MySQL source (`models/mysql-cdc`)

The mysql source is an RDS instance running in AWS that is replicating two tables.

In order to continously emit updates, events that update the tables are set up to run with a 1-second interval.

## Kafka sources (`models/loadgen`)

A Kafka broker running on the Confluent cloud is used to ingest topics that are being
fed by a `loadgen` that is running independently.

# Setup

## Environment variables

Authentication and host information is being passed into mzcompose and dbt using environment variables:

```
export MATERIALIZE_PROD_SANDBOX_HOSTNAME=...us-east-1.aws.materialize.cloud
export MATERIALIZE_PROD_SANDBOX_USERNAME=...@materialize.com
export MATERIALIZE_PROD_SANDBOX_PASSWORD=mzp_...

export MATERIALIZE_PROD_SANDBOX_RDS_HOSTNAME=...eu-west-1.rds.amazonaws.com
export MATERIALIZE_PROD_SANDBOX_RDS_PASSWORD=...
export MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_HOSTNAME=...eu-west-1.rds.amazonaws.com
export MATERIALIZE_PROD_SANDBOX_RDS_MYSQL_PASSWORD=...

export CONFLUENT_CLOUD_FIELDENG_KAFKA_BROKER=...confluent.cloud:9092
export CONFLUENT_CLOUD_FIELDENG_CSR_URL=https://...aws.confluent.cloud
export CONFLUENT_CLOUD_FIELDENG_CSR_USERNAME=...
export CONFLUENT_CLOUD_FIELDENG_CSR_PASSWORD=...
export CONFLUENT_CLOUD_FIELDENG_KAFKA_USERNAME=...
export CONFLUENT_CLOUD_FIELDENG_KAFKA_PASSWORD=...
```

## Clusters

Two clusters have been created in the sanbox environment: `qa_canary_environment_storage`
and `qa_canary_environment_compute` and all database objects are created in them. The use
of the `default` cluster is strongly discouraged.

# Creating the objects

To create or re-create the database objects:

1. Make sure the required environment variables are set

2. Run

```
./mzcompose run create
```

# Validating the workloads

the `dbt test` functionality is used to validate the correct operation of the database objects
that are being created. Most of the logic that performs the tests is in `tests/`

Among the things that are being checked is:
- whether sources and materialized views emit records on SUBSCRIBE
- the state of the sources as reported by mz_source_status table
- that all replicas are ready
- that all sources are running
- that all compute frontiers are advancing

To run the validation procedure:

1. Make sure the required environment variables are set correctly
2. Run:

```
./mzcompose run test
```
