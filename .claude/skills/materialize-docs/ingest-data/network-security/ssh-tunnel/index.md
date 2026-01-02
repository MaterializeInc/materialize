---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/network-security/ssh-tunnel/
complexity: intermediate
description: How to connect Materialize to a Kafka broker, a MySQL database, or PostgreSQL
  database using an SSH tunnel connection to a SSH bastion server
doc_type: reference
keywords:
- CREATE A
- CREATE SECRET
- CREATE SOURCE
- CREATE CONNECTION
- SSH tunnel connections
product_area: Sources
status: stable
title: SSH tunnel connections
---

# SSH tunnel connections

## Purpose
How to connect Materialize to a Kafka broker, a MySQL database, or PostgreSQL database using an SSH tunnel connection to a SSH bastion server

If you need to understand the syntax and options for this command, you're in the right place.


How to connect Materialize to a Kafka broker, a MySQL database, or PostgreSQL database using an SSH tunnel connection to a SSH bastion server


#### Cloud

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: network-security/ssh-tunnel --> --> -->

#### Self-Managed

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: network-security/ssh-tunnel-sm --> --> -->


## Create a source connection

In Materialize, create a source connection that uses the SSH tunnel connection you configured in the previous section:


#### Kafka

```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'broker1:9092',
    SSH TUNNEL ssh_connection
);
```text

You can reuse this Kafka connection across multiple [`CREATE
SOURCE`](/sql/create-source/kafka/) statements.


#### PostgreSQL

```mzsql
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection TO POSTGRES (
  HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
  PORT 5432,
  USER 'postgres',
  PASSWORD SECRET pgpass,
  SSL MODE 'require',
  DATABASE 'postgres'
  SSH TUNNEL ssh_connection
);
```text

You can reuse this PostgreSQL connection across multiple [`CREATE SOURCE`](/sql/create-source/postgres/)
statements:

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR ALL TABLES;
```json


#### MySQL

```mzsql
CREATE SECRET mysqlpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION mysql_connection TO MYSQL (
  HOST '<host>',
  SSH TUNNEL ssh_connection,
);
```

You can reuse this MySQL connection across multiple [`CREATE SOURCE`](/sql/create-source/postgres/)
statements.


## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/sql/create-source/kafka/)
- [`CREATE SOURCE`: MySQL](/sql/create-source/mysql)
- [`CREATE SOURCE`: PostgreSQL](/sql/create-source/postgres/)