# **t**ail **b**inlogs

`tb` tails database binlogs. You point it at a database, and it streams every
write that is made to the database into an [Avro Object Container File][ocf]
per table.

You can tail these files using the materialized [`AVRO OCF`][mz-avro-ocf] source
type. This lets you fire push-based events when something of interest happens
from your database in a lightweight de-coupled fashion, which would
traditionally require database triggers or something more involved.

[ocf]: https://avro.apache.org/docs/1.8.1/spec.html#Object+Container+Files
[mz-avro-ocf]: https://materialize.io/docs/sql/create-source/avro-file/

## Usage

The easiest way to get started is to use the Docker image with a mounted
directory that `materialized` has access to:

```bash
docker run --rm -v /tmp/tbshare:/tbshare materialize/tb \
    -t postgres
    -p 5432
    -d <database name>
    -H "$POSTGRES_HOST"
    -u "$POSTGRES_USER"
    -P "$POSTGRES_PASSWORD"
    --dir /tbshare/data
    --save-file /tbshare/status
```

**Important:** Ensure your database specifies the [requirements](#database-requirements)
that are detailed below.

If you only want to monitor specific tables, pass the `--whitelist` flag:

```bash
--whitelist schemaName1.tableName1,schemaName2.tableName2
```

After starting `materialized`, run the appropriate `CREATE SOURCE` command
from a SQL shell:

```sql
CREATE SOURCE my_table
FROM AVRO OCF '/tbshare/data/tb.public.my_table' WITH (tail = true)
ENVELOPE DEBEZIUM;
```

After which the standard Materialize experience will be available to you:

```sql
SHOW COLUMNS FROM my_table;
```

## Implementation

`tb` is built by [embedding Debezium][ed]. Debezium is a distributed
fault-tolerant framework for tailing binlogs into Kafka, using the Kafka
connect framework. `tb` is intended to be a lightweight tool, suitable for
running on your laptop or against a single unreplicated database. The intent is
to enable a prototyping experience that doesn't require 6 containers tethered
together in an intricate `docker-compose` setup, while still being very easily
translatable to existing Kafka infrastructure.

The exact semantics of change data capture vary based on exactly _which_
database you connect to, and what version of database you're running. `tb`
relies upon streaming replication, which is a relatively new feature in various
databases, so for best results use a recent version of your favorite database.
Streaming replication requires some configuration, but care has been taken to
keep this configuration similar to best practices for database replication. If
your database is up-to-date and running in a replicated setup, it should be
seamless to connect `tb` to it, with minimal modification.

Currently, `tb` supports PostgreSQL (10+ recommended) and MySQL.

[ed]: https://debezium.io/documentation/reference/0.10/operations/embedded.html

## Database requirements

### PostgreSQL

You should be running PostgreSQL 10+. Support for PostgreSQL <= 9 is possible,
but requires installing a plug-in such as `decoderbufs` or `wal2json` in the
database. PostgreSQL 10 onwards includes built-in streaming replication
capabilities via `pgoutput`, which is what `tb` uses out-of-the-box.

You will have to set thewWrite-ahead logging (WAL) level to logical.

```sql
ALTER SYSTEM set wal_level to logical;
```

You will need to restart your database server after changing this setting.

Tables should be created with their replica identity set to `FULL`.

```sql
CREATE TABLE table (a int, b int);
ALTER TABLE table REPLICA IDENTITY FULL;
```

### MySQL

You should be running MySQL with row-level binary logging. This means your
`my.cnf` file should have entries that look like this:

```
log_bin           = mysql-bin
binlog_format     = row
```

If you use Homebrew on macOS, the `my.cnf` file lives at
`/usr/local/etc/my.cnf`. On Linux, consult your distribution's documentation.

## Building and running

To build and run `tb`, do the following:

1. Run `mvn package`:

   ```
   mvn package
   ```

   See Maven's [Introduction to the Build Lifecycle][mvn-lifecycle] document
   for details on the Maven build system.

2. Run the newly created JAR with `java -jar target/tb-0.1-SNAPSHOT.jar [args]`.

[mvn-lifecycle]: https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html
