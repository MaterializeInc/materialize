# PostgreSQL setup w/ Debezium

To deploy Materialize using PostgreSQL as its upstream database, you'll need to
configure it with Debezium.

[Debezium] itself provides change data capture (CDC) to legacy databases like
MySQL and Postgres. Ultimately, CDC lets PostgreSQL publish to Kafka, which in turn
can be consumed by Materialize.

**!NOTE!** If you run into problems, check out the **Troubleshooting** section
at the bottom. If you don't see a solution to your problem there, throw the
answer in there once you solve it.

## Setup

### Manual

#### PostgreSQL

1. Install PostgreSQL through your favorite package manager.

    ```shell
    brew install postgres
    ```

1. Set the Write Ahead Log level (wal_level) to logical.

    ```shell
    psql
    ```

    ```postgresql
    ALTER SYSTEM set wal_level to logical;
    SHOW wal_level;
    ```

1. Restart PostgreSQL.

    ```shell
    brew services restart postgres
    ```

1. Create a `tpch` database.

    ```shell
    psql
    ```

    ```postgresql
    CREATE DATABASE tpch;
    ```

1. Create a tpch schema and a user that Debezium will connect as.

    ```postgresql
    \c tpch
    CREATE SCHEMA tpch;
    CREATE USER debezium WITH SUPERUSER PASSWORD 'debezium';
    GRANT ALL PRIVILEGES ON DATABASE "tpch" to debezium;
    GRANT ALL PRIVILEGES ON SCHEMA "tpch" to debezium;
    ```

    The debezium user requires SUPERUSER in order to create a "FOR ALL TABLES" publication.

#### Debezium + Kafka Connect

1. Download the Debezium PostgreSQL connector and place the `debezium-connector-postgres`
   directory in `/usr/local/opt/confluent-platform/share/java/`. If this directory doesn't
   exist, put the contents of the `tar` into the `share/java` directory under
   your Confluent install directory. In other words, replace
   `/usr/local/opt/confluent-platform` in the path above with the path to
   where you installed Confluent.

    ```shell
    curl -O https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/0.10.0.Final/debezium-connector-postgres-0.10.0.Final-plugin.tar.gz

    tar -zxvf debezium-connector-postgres-0.10.0.Final-plugin.tar.gz
    rm debezium-connector-postgres-0.10.0.Final-plugin.tar.gz
    if [ -d "/usr/local/opt/confluent-platform/share/java/" ]; then
      mv debezium-connector-postgres /usr/local/opt/confluent-platform/share/java
    else
      echo "Move debezium-connector-postgres into the share/java directory under your Confluent install directory"
    fi
    ```

1. Start/restart Kafka Connect.

    ```shell
    confluent local stop connect && confluent local start connect
    ```

1. Connect PostgreSQL and Kafka via Debezium.

    ```shell
    curl -H 'Content-Type: application/json' localhost:8083/connectors --data '{
      "name": "psql-connector",
      "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "localhost",
        "database.port": "5432",
        "database.user": "debezium",
        "database.password": "debezium",
        "database.dbname" : "tpch",
        "database.server.name": "tpch",
        "plugin.name": "pgoutput",
        "slot.name" : "tester",
        "database.history.kafka.bootstrap.servers": "localhost:9092",
        "database.history.kafka.topic": "tpch-history"
      }
    }'
    ```

    If you get an error that the Debezium PostgreSQL connector doesn't exist, check
    out the `Failed to find any class that implements Connector and which name
    matches io.debezium.connector.postgresql.PostgresConnector` section below.

1. Watch the Connect log file and look for messages from Debezium.

    ```shell
    confluent local log connect
    ```

## Loading TPCH

To test that your local PostgreSQL/Debezium actually works with Materialize, we'll
have Materialize ingest the TPCH `lineitem` table.

1. Generate and load TPC-H data with [`tpch-gen`](https://github.com/MaterializeInc/tpch-dbgen.git).

    ```bash
    git clone git@github.com:MaterializeInc/tpch-dbgen.git
    cd tpch-dbgen
    make
    ./dbgen

    ./generate_postgres_data.sh
    psql -f postgres_ddl.sql postgresql://debezium:debezium@localhost/tpch
    ```

    **Note that Debezium versions before 1.0 mishandled dates in a way that
    can cause incorrect results when loading data into Materialize.**

1.  In a new shell (#2), read the actual data out of Kafka once Debezium
    finishes its first snapshot.

    ```shell
    kafka-avro-console-consumer --from-beginning --bootstrap-server localhost:9092 --topic tpch.tpch.lineitem
    ```

1. In a new shell (#3), launch a `materialized` server.

    ```shell
    cd <path/to/materialized> && cargo run --bin materialized
    ```

1. In a new shell (#4), connect to `materialized` and create a source to import the `lineitems` table.

    ```shell
    cd <path/to/materialized>
    source doc/developer/assets/demo/utils.sh
    mtrlz-shell
    ```

    ```sql
    CREATE SOURCE lineitem
    FROM KAFKA BROKER 'localhost:9092' TOPIC 'tpch.tpch.lineitem'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
    CREATE MATERIALIZED VIEW count AS SELECT COUNT(*) FROM lineitem;
    SELECT * FROM count;
    SELECT * FROM count;
    -- ...
    ```

    Once `SELECT * FROM count` returns `6001215`, all of the rows from
    `lineitem` have been imported.

    Naturally, if this works at all, it indicates that Debezium is dutifully
    getting data out of PostgreSQL. You could, at this point, tear down the setup
    that is loading `lineitem` into Materialize.

## Troubleshooting

### `Failed to find any class that implements Connector and which name matches io.debezium.connector.postgresql.PostgresConnector`

If you encounter this error, the `confluent-platform` set of tools that you
installed cannot find the `debezium-connector-postgres` directory.

To start investigating this, find out where `confluent-platform` is looking for
files to include using...

```
confluent local log connect
```

There's a chance it'll be `~/Downloads/` or some other such nonsense.

To fix this particular version of this issue, remove any reference to the
`confluent` or `confluent-platform` binary in the offending location (e.g. `tar`
files), and then restart Kafka Connect.

```shell
confluent local stop connect && confluent local start connect
```
