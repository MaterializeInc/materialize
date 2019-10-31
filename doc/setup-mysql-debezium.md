# MySQL setup w/ Debezium

To deploy Materialize using MySQL as its upstream database, you'll need to
configure it with Debezium.

[Debezium] itself provides change data capature (CDC) to legacy databases like
MySQL and Postgres. Ultimately, CDC lets MySQL publish to Kafka, which in turn
can be consumed by Materialize.

## Setup

### Scripted

#### Mac/Homebrew

Check out [`mtrlz-setup`](https://github.com/MaterializeInc/mtrlz-setup)'s
`debezium.sh`.

### Manual

1. Install MySQL through your favorite package manager.

    ```shell
    brew install mysql
    ```

1. Adjust the MySQL configuration file to use UTC by adding the following line
   to 'my.cnf`:

    ```shell
    default-time-zone = '+00:00'
    ```

    With Homebrew, the file lives at `/usr/local/etc/my.cnf`.

1. Restart MySQL that it can pick up the time zone change.

    ```shell
    brew services restart mysql
    ```

1. Create a `tpch` database.

    ```shell
    mysql -uroot
    ```

    ```mysql
    CREATE DATABASE tpch;
    ```

1. Create a MySQL user that Debezium will connect as.

    ```mysql
    CREATE USER debezium;
    ALTER USER debezium IDENTIFIED WITH mysql_native_password BY 'debezium';
    GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT  ON *.* TO 'debezium';
    GRANT ALL PRIVILEGES ON tpch.* TO 'debezium'@'%';
    ```

    The `IDENTIFIED WITH mysql_native_password ...` clause sets the password for
    the account to Debezium using the old MySQL password protocol, which is all
    that Debezium supports. This is only necessary on MySQL 8.x+ (I think); if
    you get an error about `mysql_native_password` not being a thing, try just
    dropping the `WITH mysql_native_password` bit from the query.

1. Load TPC-H with `benesch`'s fork of [`tpch-gen`](https://github.com/benesch/tpch-dbgen)'s data.

    ```bash
    curl -O https://materialize-ddls.s3.amazonaws.com/tpch-mysql.tar.gz
    tar -zxf tpch-mysql.tar.gz
    rm tpch-mysql.tar.gz
    cd tpch-mysql
    mysql -uroot --local-infile < mysql-tpch-ddl.sql
    cd ../
    rm -rf tpch-mysql
    ```

    You can also download the repo and generate these DDLs on the fly.

    ```bash
    git clone https://github.com/benesch/tpch-dbgen.git
    cd tpch-dbgen
    make
    ./dbgen
    mysql -uroot --local-infile < ddl.sql
    ```

1. Download the [latest stable Debezium MySQL
   connector](https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/0.9.5.Final/debezium-connector-mysql-0.9.5.Final-plugin.tar.gz)
   and place the `debezium-connector-mysql` directory in
   `/usr/local/opt/confluent-platform/share/java/`. If this directory doesn't
   exist, put the contents of the `tar` into the `share/java` directory under
   your Confluent install directory. In other words, replace
   `/usr/local/opt/confluent-platform` in the path above with the path to
   where you installed Confluent.

    ```shell
    curl -O https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/0.9.5.Final/debezium-connector-mysql-0.9.5.Final-plugin.tar.gz
    tar -zxvf debezium-connector-mysql-0.9.5.Final-plugin.tar.gz
    rm debezium-connector-mysql-0.9.5.Final-plugin.tar.gz
    if [ -d "/usr/local/opt/confluent-platform/share/java/" ]; then
      mv debezium-connector-mysql /usr/local/opt/confluent-platform/share/java
    else
      echo "Move debezium-connector-mysql into the share/java directory under your Confluent install directory"
    fi
    ```

## Loading TPCH

To test that your local MySQL/Debezium actually works with Materialize, we'll
have Materialize ingest the TPCH `lineitem` table.

**!NOTE!** If you run into problems, check out the **Troubleshooting** section
at the bottom. If you don't see a solution to your problem there, throw the
answer in their once you solve it.

1. Start/restart Kafka Connect.

    ```shell
    confluent local stop connect && confluent local start connect
    ```

1. Connect MySQL and Kafka via Debezium.

    ```shell
    curl -H 'Content-Type: application/json' localhost:8083/connectors --data '{
        "name": "tpch-connector",
        "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": "localhost",
        "database.port": "3306",
        "database.user": "debezium",
        "database.password": "debezium",
        "database.server.name": "tpch",
        "database.server.id": "184054",
        "database.history.kafka.bootstrap.servers": "localhost:9092",
        "database.history.kafka.topic": "tpch-history",
        }
    }'
    ```

    If you get an error that the Debezium MySQL connector doesn't exist, check
    out the `Failed to find any class that implements Connector and which name
    matches io.debezium.connector.mysql.MySqlConnector` section below.

1. Watch the Connect log file and look for messages from Debezium.

    ```shell
    confluent local log connect
    ```

1.  In a new shell (#2), read the actual data out of Kafka once Debezium
    finishes its first snapshot.

    ```shell
    kafka-avro-console-consumer --from-beginning --bootstrap-server localhost:9092 --topic tpch.tpch.customer
    ```

1. In a new shell (#3), launch a `materialized` server.

    ```shell
    cd <path/to/materialized> && cargo run --bin materialized
    ```

1. In a new shell (#4), connect to `materialized` and create a source to import the `lineitems` table.

    ```shell
    cd <path/to/materialized>
    source doc/demo-utils.sh
    mtrlz-shell
    ```

    ```sql
    CREATE SOURCE lineitem FROM 'kafka://localhost:9092/tpch.tpch.lineitem' USING SCHEMA REGISTRY 'http://localhost:8081';
    CREATE MATERIALIZED VIEW count AS SELECT COUNT(*) FROM lineitem;
    PEEK count;
    PEEK count;
    -- ...
    ```

    Once `PEEK count` returns `6001215`, all of the rows from `lineitem` have
    been imported.

    Naturally, if this works at all, it indicates that Debezium is dutifully
    getting data out of MySQL. You could, at this point, tear down the setup
    that is loading `lineitem` into Materialize.

## Troubleshooting

### `Failed to find any class that implements Connector and which name matches io.debezium.connector.mysql.MySqlConnector`

If you encounter this error, the `confluent-platform` set of tools that you
installed cannot find the `debezium-connector-mysql` directory.

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

### Missing rows

If `PEEK count` stops growing at a value less than `6001215`, try writing
explicit watermarks to indicate that a topic is finished. For example, to add a
watermark to lineitems:

    ```shell
    kafka-avro-console-producer --broker-list localhost:9092 --topic tpch.tpch.lineitem --property value.schema="$(curl localhost:8081/subjects/tpch.tpch.lineitem-value/versions/1 | jq -r .schema | jq)" <<<'{"before":null,"after":null,"source":{"version":{"string":"0.9.5.Final"},"connector":{"string":"mysql"},"name":"tpch","server_id":0,"ts_sec":0,"gtid":null,"file":"binlog.000004","pos":951896181,"row":0,"snapshot":{"boolean":true},"thread":null,"db":{"string":"tpch"},"table":{"string":"lineitem"},"query":null},"op":"c","ts_ms":{"long":1560886948093}}'
    ```
